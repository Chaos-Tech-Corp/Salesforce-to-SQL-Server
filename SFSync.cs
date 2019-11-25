using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;


class SFSync
{

    private protected SalesforceClient _client;
    private protected Database _db;
    private protected string _databaseName;
    private protected string _schemaData = "data";
    private protected string _schemaObjects = "dbo";

    public SFSync(string userName, string password, string token, string key, string secret, string ApiVersion = "v44.0", Boolean isSandbox = false)
    {
        _client = new SalesforceClient(key, secret, userName, password, token, isSandbox, ApiVersion);
    }

    public void Connect(string connectionString, string databaseName)
    {
        _db = new Database(connectionString);
        _databaseName = databaseName;
    }

    public void Start()
    {
        if (_db == null)
        {
            throw new Exception("Database not initialized.");
        }

        //create the SF client and connect to the instance
        _client.CreateClientAsync().Wait();

        var existingObjects = ProcessObjectMetadata();
        var tablesToSkip = ObjectsToSkip();

        //if you want to delete any tables not realted to existing salesforce objects
        RemoveTables(existingObjects);

        foreach (var o in existingObjects.OrderBy(O => O.QualifiedApiName))
        {

            Boolean isHistoryTable = o.QualifiedApiName != "LoginHistory" && o.QualifiedApiName.EndsWith("History");

            //filter the type of objects to synchronize
            if (o.IsQueryable && !o.IsDeprecatedAndHidden && !o.IsCustomSetting) //&& o.IsLayoutable 
            {
                //do not sync Feed, Share and Tag tables for the objects.
                if (o.QualifiedApiName.EndsWith("Feed") || o.QualifiedApiName.EndsWith("Share") || o.QualifiedApiName.EndsWith("Tag"))
                {
                    continue;
                }

                string fixedName = o.QualifiedApiName;
                if (isHistoryTable)
                {
                    fixedName = o.QualifiedApiName.Substring(0, o.QualifiedApiName.Length - 7);
                    if (fixedName.EndsWith("__")) fixedName += "__c";
                }

                if (tablesToSkip.Contains(o.QualifiedApiName) || tablesToSkip.Contains(fixedName))
                {
                    //delete the tbable in case it exists
                    if (_db.ExistTable(_schemaObjects, o.QualifiedApiName))
                    {
                        _db.ExecuteSQL("DROP TABLE [" + _schemaObjects + "].[" + o.QualifiedApiName + "]");
                    }
                    continue;
                }

                //Console.WriteLine(o.Label + " | " + o.QualifiedApiName + " | " + o.IsQueryable);

                string dateFieldName ;
                var oDefinition = UpdateObjectDefinition(o, out dateFieldName);


                //remove fields that we don't want to download. --> this will exclude BLOB and long text fields
                oDefinition.fields = oDefinition.fields.Where(F => F.type != "base64" && (F.type != "textarea" || (F.type == "textarea" && F.length <= 32768))).ToList();


                bool isNewTable = false;
                if (!_db.ExistTable(_schemaObjects, o.QualifiedApiName))
                {
                    isNewTable = true;
                }
                else
                {
                    //if there are any new fields, recreate the table
                    var existingColumns = _db.GetDataTable("select * from [" + _schemaObjects + "].[" + o.QualifiedApiName + "] where 1 = 2").Columns;

                    foreach (var field in oDefinition.fields)
                    {
                        Boolean found = false;
                        foreach (DataColumn c in existingColumns)
                        {
                            if (c.ColumnName.ToLower() == field.name.ToLower())
                            {
                                found = true;
                                break;
                            }
                        }
                        if (!found)
                        {
                            isNewTable = true;
                            break;
                        }
                    }

                    if (isNewTable)
                    {
                        _db.ExecuteSQL("drop table [" + _schemaObjects + "].[" + o.QualifiedApiName + "]");
                    }
                }

                if (isNewTable)
                {
                    CreateTable(o.QualifiedApiName, oDefinition);
                }

                //find the last modified/moodstamp date for this object
                object lastSync = null;
                if (!string.IsNullOrEmpty(dateFieldName))
                {
                    lastSync = _db.ExecuteScalar("select top 1 [" + dateFieldName + "] from [" + _schemaObjects + "].[" + o.QualifiedApiName + "] order by [" + dateFieldName + "] desc");
                }

                bool queryAll = !oDefinition.fields.Any(F => F.name == "IsDeleted"); //use QueryAll only if there is no IsDeleted Field

                //if there is no lastsync might need to load records a bit at a time
                long totalCount = 0;
                long currentCount = -1;
                if (lastSync == null && (!o.IsLayoutable || o.QualifiedApiName == "ContentVersion" || o.QualifiedApiName == "ContentDocument" || o.QualifiedApiName == "ContentDocumentLink")) // ensure content documents go through this piece
                {
                    while (true)
                    {
                        try
                        {
                            var totalCountQuery = _client.Client.QueryAsync<dynamic>("SELECT COUNT() FROM " + o.QualifiedApiName + (queryAll ? "" : " WHERE IsDeleted = False")).Result;
                            totalCount = totalCountQuery.TotalSize;
                            break;
                        }
                        catch
                        {
                            System.Threading.Thread.Sleep(1000);
                        }
                    }

                    Console.WriteLine(" | Total Count: " + totalCount);
                    if (totalCount < 5000)
                    {
                        totalCount = 0;
                    }
                }
                else
                {
                    Console.WriteLine();
                }

                string lastId = null;
                string query;
                string nextPageUrl = null;
                List<dynamic> records = null;
                int pageCount = 0;
                List<string> idList = null;

                //if no timestamp check the latest inserted id
                if (string.IsNullOrEmpty(dateFieldName))
                {
                    var lastFoundId = _db.ExecuteScalar("select top 1 [Id] from [" + _schemaObjects + "].[" + o.QualifiedApiName + "] order by [Id] desc");
                    if (lastFoundId != null)
                    {
                        lastId = lastFoundId.ToString();
                    }
                }

                //limit only certain objects
                Boolean useLimit = (o.QualifiedApiName == "ContentVersion" || o.QualifiedApiName == "ContentDocument" || o.QualifiedApiName == "ContentDocumentLink");

                /* Find New / Updated Records*/
                /*===========================*/
                while (currentCount < totalCount)
                {

                    query = "SELECT " + string.Join(",", oDefinition.fields.Select(D => D.name)) + " FROM " + o.QualifiedApiName;


                    if (totalCount > 0)
                    {
                        if (!string.IsNullOrEmpty(lastId))
                        {
                            if (!string.IsNullOrEmpty(dateFieldName))
                            {
                                lastSync = _db.ExecuteScalar("select top 1 [" + dateFieldName + "] from [" + _schemaObjects + "].[" + o.QualifiedApiName + "] order by [" + dateFieldName + "] desc");
                                //need to refresh the latest record inserted
                                query += " WHERE " + dateFieldName + " >= " + ((DateTime)lastSync).ToString("yyyy-MM-ddTHH:mm:ssZ") + " AND  ID > '" + lastId + "' ";
                            }
                            else
                            {
                                lastSync = _db.ExecuteScalar("select top 1 ID from [" + _schemaObjects + "].[" + o.QualifiedApiName + "] order by ID desc");
                                //need to refresh the latest record inserted
                                query += " WHERE ID > '" + lastId + "' ";
                            }

                        }
                        if (!string.IsNullOrEmpty(dateFieldName))
                        {
                            query += " ORDER BY " + dateFieldName + ", ID ASC " + (useLimit ? "LIMIT 5000" : "");
                        }
                        else
                        {
                            query += " ORDER BY ID ASC " + (useLimit ? "LIMIT 5000" : ""); ;
                        }
                    }
                    else
                    {
                        if (lastSync != null)
                        {
                            if (!string.IsNullOrEmpty(dateFieldName))
                            {
                                query += " WHERE " + dateFieldName + " > " + ((DateTime)lastSync).ToString("yyyy-MM-ddTHH:mm:ssZ") + "";
                            }
                            else
                            {
                                query += " WHERE Id > '" + lastSync.ToString() + "'";
                            }
                        }
                        if (!string.IsNullOrEmpty(dateFieldName))
                        {
                            query += " ORDER BY " + dateFieldName + " ASC";
                        }
                        else
                        {
                            query += " ORDER BY ID ASC";
                        }
                    }

                    nextPageUrl = null;
                    records = null;
                    pageCount = 0;
                    idList = null;


                    while (!string.IsNullOrEmpty(nextPageUrl) || pageCount == 0)
                    {
                        pageCount++;
                        Tuple<List<dynamic>, string> recordsQuery;
                        while (true)
                        {
                            try
                            {
                                recordsQuery = _client.GetRecordsPaged(query, nextPageUrl, queryAll).Result;
                                break;
                            }
                            catch (Exception ex)
                            {
                                //ignore the exception and keep trying
                                //too many times SF times out
                            }
                        }
                        records = recordsQuery.Item1;
                        nextPageUrl = recordsQuery.Item2;

                        //get an empty data table to populate from the records retrieved
                        DataTable dt = _db.GetDataTable("select * from [" + _schemaObjects + "].[" + o.QualifiedApiName + "] where 1 = 2");

                        foreach (dynamic entry in records)
                        {
                            var row = dt.NewRow();
                            foreach (var field in oDefinition.fields)
                            {
                                if (entry[field.name] is Newtonsoft.Json.Linq.JObject) continue;
                                if (entry[field.name].Value is DateTime)
                                {
                                    row[field.name] = ((DateTime)entry[field.name].Value).ToUniversalTime();
                                }
                                else
                                {
                                    row[field.name] = entry[field.name].Value ?? DBNull.Value;
                                }
                                if (DataType(field).StartsWith("varchar"))
                                {
                                    if (row[field.name] != DBNull.Value && field.length > 0 && row[field.name].ToString().Length > field.length)
                                    {
                                        row[field.name] = row[field.name].ToString().Substring(0, field.length);
                                    }
                                }
                            }
                            dt.Rows.Add(row);
                            lastId = row["Id"].ToString();
                        }
                        records.Clear();


                        idList = new List<string>();
                        if (dt.Rows.Count > 0)
                        {
                            //delete any existing matches
                            if ((lastSync != null || lastId != null) && !isNewTable)
                            {
                                foreach (DataRow r in dt.Rows)
                                {
                                    idList.Add(r["id"].ToString());
                                    if (idList.Count > 50)
                                    {
                                        _db.ExecuteSQL("delete from [" + _schemaObjects + "].[" + o.QualifiedApiName + "] where ID in ('" + string.Join("','", idList) + "')");
                                        idList.Clear();
                                    }
                                }
                                if (idList.Count > 0)
                                {
                                    _db.ExecuteSQL("delete from [" + _schemaObjects + "].[" + o.QualifiedApiName + "] where ID in ('" + string.Join("','", idList) + "')");
                                }
                            }

                            dt.TableName = o.QualifiedApiName;
                            _db.InsertDataTable(dt);
                        }

                        currentCount += dt.Rows.Count;
                        if (dt.Rows.Count == 0 || currentCount == totalCount - 1)
                        {
                            currentCount = totalCount;
                        }

                    }
                    GC.Collect();


                }



                /* Find Deleted Records*/
                /*=====================*/

                if (!oDefinition.fields.Any(F => F.name.ToLower() == "IsDeleted".ToLower())) continue; //if not isdeleted field then continue
                if (isNewTable) continue;

                query = "SELECT Id FROM " + o.QualifiedApiName + " WHERE IsDeleted = TRUE ";

                if (lastSync != null)
                {
                    query = query + " AND " + dateFieldName + " > " + ((DateTime)lastSync).ToString("yyyy-MM-ddTHH:mm:ssZ") + "";
                }

                nextPageUrl = null;
                records = null;
                pageCount = 0;
                idList = null;

                while (!string.IsNullOrEmpty(nextPageUrl) || pageCount == 0)
                {
                    pageCount++;
                    var recordsQuery = _client.GetRecordsPaged(query, nextPageUrl, true).Result;
                    records = recordsQuery.Item1;
                    nextPageUrl = recordsQuery.Item2;

                    idList = new List<string>();
                    foreach (dynamic entry in records)
                    {
                        idList.Add(entry["Id"].ToString());
                        if (idList.Count > 100)
                        {
                            _db.ExecuteSQL("delete from [" + _schemaObjects + "].[" + o.QualifiedApiName + "] where [Id] in ('" + string.Join("','", idList) + "')");
                            idList.Clear();
                        }
                    }
                    if (idList.Count > 0)
                    {
                        _db.ExecuteSQL("delete from [" + _schemaObjects + "].[" + o.QualifiedApiName + "] where [Id] in ('" + string.Join("','", idList) + "')");
                    }

                }

            }
            GC.Collect();


        }


    }

    /// <summary>
    /// Obtain the Metadata from SF and update (or create) the local tables with all the metadata of the objects
    /// </summary>
    public List<EntityDefinition> ProcessObjectMetadata()
    {
        var objects = _client.QueryRecords<EntityDefinition>("SELECT QualifiedApiName,IsSearchable,IsRetrieveable, DeveloperName, IsFeedEnabled, IsCustomizable, DurableId, IsQueryable, IsLayoutable, IsDeprecatedAndHidden, IsCustomSetting, KeyPrefix, Label, NamespacePrefix FROM EntityDefinition").Result;

        if (!_db.ExistTable(_schemaData, "EntityDefinition"))
        {
            _db.ExecuteSQL(@"create table [sf_sync].[EntityDefinition] (
                QualifiedApiName varchar(128) not null,
                DeveloperName varchar(128) not null,
                DurableId varchar(64) not null,
                IsCustomizable bit not null,
                IsFeedEnabled bit not null,
                IsQueryable bit not null,
                IsLayoutable bit not null,
                IsDeprecatedAndhidden bit not null,
                IsCustomSetting bit not null,
                IsRetrieveable bit not null,
                IsSearchable bit not null,
                KeyPrefix varchar(3) null,
                Label varchar(255) null,
                NamespacePrefix varchar(32) null
            )");
        }
        if (!_db.ExistTable(_schemaData, "ObjectDescription"))
        {
            _db.ExecuteSQL(@"create table [sf_sync].[ObjectDescription] (
                ObjectName varchar(128) not null,
                Label varchar(128) not null,
                Name varchar(128) not null,
                Length varchar(64) not null,
                Type varchar(64) not null,
                Updateable bit not null,
                Calculated bit not null,
                Digits int null,
                Precision int null,
                Scale int null,
                IdLookup bit null,
                AutoNumber bit null,
                ReferenceTo varchar(max)
            )");
        }
        _db.ExecuteSQL("truncate table [" + _schemaData + "].[EntityDefinition]");
        _db.ExecuteSQL("truncate table [" + _schemaData + "].[ObjectDescription]");

        DataTable dt_EntityDef = _db.GetDataTable("SELECT * FROM [" + _schemaData + "].[EntityDefinition]");
        dt_EntityDef.Namespace = _schemaData;
        dt_EntityDef.TableName = "EntityDefinition";
        foreach (var o in objects)
        {
            var nr = dt_EntityDef.NewRow();
            nr["DeveloperName"] = o.DeveloperName;
            nr["DurableId"] = o.DurableId;
            nr["IsCustomizable"] = o.IsCustomizable;
            nr["IsCustomSetting"] = o.IsCustomSetting;
            nr["IsDeprecatedAndHidden"] = o.IsDeprecatedAndHidden;
            nr["IsFeedEnabled"] = o.IsFeedEnabled;
            nr["IsLayoutable"] = o.IsLayoutable;
            nr["IsQueryable"] = o.IsQueryable;
            nr["IsRetrieveable"] = o.IsRetrieveable;
            nr["IsSearchable"] = o.IsSearchable;
            nr["KeyPrefix"] = o.KeyPrefix;
            nr["Label"] = o.Label;
            nr["NamespacePrefix"] = o.NamespacePrefix;
            nr["QualifiedApiName"] = o.QualifiedApiName;
            dt_EntityDef.Rows.Add(nr);
        }
        _db.InsertDataTable(dt_EntityDef);

        return objects;
    }


    public ObjectDescription UpdateObjectDefinition(EntityDefinition o, out string dateFieldName)
    {
        var oDefinition = _client.Client.DescribeAsync<ObjectDescription>(o.QualifiedApiName).Result;

        //update the definition of the object
        DataTable dt_ObjectDef = _db.GetDataTable("SELECT * FROM [" + _schemaData + "].[ObjectDescription] WHERE 1 = 2");
        dt_ObjectDef.Namespace = _schemaData;
        dt_ObjectDef.TableName = "ObjectDescription";
        foreach (var f in oDefinition.fields)
        {
            var refTo = string.Join(",", f.referenceTo);
            if (string.IsNullOrEmpty(refTo)) refTo = null;

            var nr = dt_ObjectDef.NewRow();
            nr["ObjectName"] = o.QualifiedApiName;
            nr["Label"] = f.label;
            nr["Name"] = f.name;
            nr["Length"] = f.length;
            nr["Type"] = f.type;
            nr["Updateable"] = f.updateable;
            nr["Calculated"] = f.calculated;
            nr["Digits"] = f.digits;
            nr["Precision"] = f.precision;
            nr["Scale"] = f.scale;
            nr["IdLookup"] = f.idLookup;
            nr["AutoNumber"] = f.autoNumber;
            nr["ReferenceTo"] = refTo;
            dt_ObjectDef.Rows.Add(nr);
        }

        string systemMoodStamp = "SystemModstamp".ToLower();
        dateFieldName = null;

        //if not contains the systemmodstamp, we don't query the table
        var hasMoodStamp = oDefinition.fields.Any(F => F.name.ToLower() == systemMoodStamp);

        if (!hasMoodStamp)
        {
            if (oDefinition.fields.Any(F => F.name.ToLower() == "LastModifiedDate".ToLower()))
            {
                dateFieldName = "LastModifiedDate";
            }
            else if (oDefinition.fields.Any(F => F.name.ToLower() == "CreatedDate".ToLower()))
            {
                dateFieldName = "CreatedDate";
            }
            else if (oDefinition.fields.Any(F => F.name.ToLower() == "LoginTime".ToLower()))
            {
                dateFieldName = "LoginTime";
            }
        }

        //if the object has no date field to use, skip
        if (string.IsNullOrEmpty(dateFieldName))
        {
            return null;
        }

        //insert the object definition into the table
        _db.InsertDataTable(dt_ObjectDef);
        //if you want to include the object metadata regardless, move this up.

        // update to use the new dateFieldName value
        return oDefinition;

    }

    public void RemoveTables(List<EntityDefinition> objects)
    {
        var existing_tables = _db.GetRecords<schema_table>("select TABLE_NAME from information_Schema.tables where TABLE_SCHEMA = '" + _schemaObjects + "' and TABLE_TYPE = 'BASE TABLE'");
        foreach (var table2delete in existing_tables.Where(E => !objects.Any(O => O.QualifiedApiName.ToLower() == E.TABLE_NAME.ToLower())))
        {
            _db.ExecuteSQL("DROP TABLE [" + _schemaObjects + "].[" + table2delete.TABLE_NAME + "]");
        }
    }

    /// <summary>
    ///  List of all the objects that do not need to be synchronized (manually added to the [TablesToSkip] table)
    /// </summary>
    public List<string> ObjectsToSkip()
    {
        List<string> objectsToSkip = new List<string>();
        foreach (DataRow r in _db.GetDataTable("SELECT * FROM [" + _schemaData + "].[TablesToSkip]").Rows)
        {
            objectsToSkip.Add(r["TableName"].ToString());
        }
        return objectsToSkip;
    }

    /// <summary>
    /// Create a Table from an Object
    /// </summary>
    void CreateTable(string name, ObjectDescription description, DataColumnCollection newColumns = null)
    {
        if (newColumns != null)
        {
            _db.ExecuteSQL("drop table [" + _schemaObjects + "].[" + name + "]");
        }

        string query = "create table [" + _schemaObjects + "].[{0}] (";
        List<string> columns = new List<string>();

        foreach (var item in description.fields.OrderBy(F => F.name))
        {
            columns.Add("[" + item.name + "] " + DataType(item));
        }

        query = query + string.Join(",", columns) + ")";

        _db.ExecuteSQL(query.Replace("{0}", name));


        if (description.fields.Any(F => F.name.ToLower() == "SystemModstamp".ToLower()))
        {
            _db.ExecuteSQL("CREATE INDEX [I_" + name + "_SM] ON [" + name + "] ([SystemModstamp])");
        }


    }

    /// <summary>
    /// Equivalence between Salesforce and SQL Server data types
    /// </summary>
    /// <param name="field"></param>
    /// <returns></returns>
    static string DataType(fields field)
    {
        switch (field.type)
        {
            case "picklist": return "varchar(" + field.length + ") null";
            case "id": return "varchar(" + field.length + ") not null PRIMARY KEY";
            case "datetime": return "datetime null";
            case "reference": return "varchar(" + field.length + ") null";
            case "boolean": return "bit null";
            case "string": return "varchar(" + field.length + ") null";
            case "date": return "datetime null";
            case "textarea": return "varchar(max) null";
            case "double": return "decimal (18,4) null";
            case "address": return "varchar(max) null";
            case "int": return "int null";
            case "multipicklist": return "varchar(max) null";
            case "anyType": return "varchar(max) null";
            case "url": return "varchar(" + field.length + ") null";
            case "currency": return "decimal (18,4) null";
            case "percent": return "decimal (6,3) null";
            case "phone": return "varchar(" + field.length + ") null";
            case "email": return "varchar(" + field.length + ") null";
            case "base64": return "varchar(max) null";
            case "combobox": return "varchar(255) null";
            case "time": return "datetime null";
            case "encryptedstring": return "varchar(max) null";
            case "complexvalue": return "varchar(max) null";

        }

        return "varchar(max) null";
    }

}
