/*MIT License

Copyright (c) 2013-2018 Javier Chaos

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.*/

using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Web;


public class Database : IDisposable
{
    private SqlConnection _conn;
    private SqlTransaction _tran;
    private String _connectionString;
    private string _databaseName = null;

    public string ConnectionString
    {
        get { return _connectionString; }
    }

    public Database(string connectionString)
    {
        _connectionString = connectionString;
        _conn = new SqlConnection(connectionString + ";MultipleActiveResultSets=true");

        _conn.Open();

    }

    public void Dispose()
    {
        if (_conn.State != ConnectionState.Closed)
        {
            // cancel any pending transaction;
            this.RollBack();

            //_conn.Close();
        }
        _conn.Dispose();

        cachedQueries.Clear();

    }

    public Boolean IsOpen
    {
        get
        {
            return (_conn != null && _conn.State == ConnectionState.Open);
        }
    }

    public void BeginTransaction()
    {
        _tran = _conn.BeginTransaction(IsolationLevel.Serializable);
    }

    public void Commit()
    {
        if (_tran != null)
        {
            _tran.Commit();
            _tran.Dispose();
            _tran = null;
        }
    }

    public void RollBack()
    {
        if (_tran != null)
        {
            _tran.Rollback();
            _tran.Dispose();
            _tran = null;
        }
    }

    private SqlCommand CreateCommand(string query, params KeyValuePair<string, object>[] Parameters)
    {
        SqlCommand comm;

        if (_tran != null)
            comm = new SqlCommand(query, _conn, _tran);
        else
            comm = new SqlCommand(query, _conn);

        if (Parameters != null)
        {
            foreach (KeyValuePair<string, object> pair in Parameters)
            {

                if (pair.Value == null)
                {
                    comm.Parameters.AddWithValue(pair.Key, DBNull.Value);
                }
                else
                {
                    comm.Parameters.AddWithValue(pair.Key, pair.Value);
                }
                if (pair.Key.ToLower().StartsWith("@output_"))
                {
                    comm.Parameters[pair.Key].Direction = ParameterDirection.Output;
                }
            }
        }
        comm.CommandTimeout = 0; // seconds timeout
        return comm;
    }

    public SqlDataReader CreateReader(string query)
    {
        using (SqlCommand comm = CreateCommand(query))
        {
            return comm.ExecuteReader();
        }
    }

    public string GetDatabaseName(Boolean force = false)
    {
        if (string.IsNullOrEmpty(_databaseName) || force)
        {
            _databaseName = ExecuteScalar("select DB_NAME()").ToString();
        }
        return _databaseName;
    }

    public string GetDatabasePath(string DatabaseName = "")
    {
        if (string.IsNullOrEmpty(DatabaseName)) DatabaseName = GetDatabaseName();

        string path = ExecuteScalar("SELECT top 1 F.physical_name AS current_file_location FROM sys.master_files  F inner join sys.databases D on D.database_id = F.database_id where D.name = '" + DatabaseName + "' and F.Type_DESC = 'ROWS'").ToString();
        return System.IO.Path.GetDirectoryName(path);
    }

    public bool ExistSchema(string schema)
    {
        return Convert.ToInt32(ExecuteScalar("select count(*) from information_schema.schemata where schema_name = '" + schema + "'")) > 0;
    }

    public bool ExistTable(string schema, string tableName, string database = null)
    {
        database = string.IsNullOrEmpty(database) ? "" : "[" + database + "].";
        return 1 == Convert.ToInt32(ExecuteScalar("select count(*) from " + database + "information_schema.tables where table_schema = '" + schema + "' and table_name = '" + tableName + "'"));
    }

    public void ExecuteProcedure(string procName, SortedDictionary<string, object> Parameters)
    {
        List<KeyValuePair<string, object>> kvParam = new List<KeyValuePair<string, object>>();
        foreach (var param in Parameters)
        {
            kvParam.Add(new KeyValuePair<string, object>(param.Key, param.Value));
        }
        ExecuteProcedure(procName, kvParam.ToArray());
    }

    public void ExecuteProcedure(string procName, params KeyValuePair<string, object>[] Parameters)
    {
        using (SqlCommand comm = CreateCommand(procName, Parameters))
        {
            comm.CommandTimeout = 60 * 10; // 10 minutes
            comm.CommandType = CommandType.StoredProcedure;

            comm.ExecuteNonQuery();
        }
    }

    public IAsyncResult BeginExecuteProcedure(string procName, params KeyValuePair<string, object>[] Parameters)
    {
        using (SqlCommand comm = CreateCommand(procName, Parameters))
        {
            comm.CommandTimeout = 60 * 10; // 10 minutes
            comm.CommandType = CommandType.StoredProcedure;

            return comm.BeginExecuteNonQuery(delegate (IAsyncResult ar) { try { comm.EndExecuteNonQuery(ar); } catch { } }, null);
        }
    }

    public int ExecuteSQL(string query, SortedDictionary<string, object> Parameters)
    {
        List<KeyValuePair<string, object>> kvParam = new List<KeyValuePair<string, object>>();
        foreach (var param in Parameters)
        {
            kvParam.Add(new KeyValuePair<string, object>(param.Key, param.Value));
        }
        return ExecuteSQL(query, kvParam.ToArray());
    }

    public int ExecuteSQL(string query, params KeyValuePair<string, object>[] Parameters)
    {
        using (SqlCommand comm = CreateCommand(query, Parameters))
        {
            return comm.ExecuteNonQuery();
        }
    }

    public int TryExecuteSQL(string query, params KeyValuePair<string, object>[] Parameters)
    {
        try
        {
            return ExecuteSQL(query, Parameters);
        } catch
        {
            return -1;
        }
    }

    public int TryExecuteSQL(string query, SortedDictionary<string, object> Parameters)
    {
        try
        {
            return ExecuteSQL(query, Parameters);
        }
        catch
        {
            return -1;
        }
    }

    public object ExecuteInsertSQL(string query, SortedDictionary<string, object> Parameters)
    {
        List<KeyValuePair<string, object>> kvParam = new List<KeyValuePair<string, object>>();
        foreach (var param in Parameters)
        {
            kvParam.Add(new KeyValuePair<string, object>(param.Key, param.Value));
        }
        return ExecuteInsertSQL(query, kvParam.ToArray());
    }

    public object ExecuteInsertSQL(string query, params KeyValuePair<string, object>[] Parameters)
    {
        using (SqlCommand comm = CreateCommand(query + ";select scope_identity();", Parameters))
        {
            return comm.ExecuteScalar();
        }
    }

    public object ExecuteScalar(string query, SortedDictionary<string, object> Parameters)
    {
        List<KeyValuePair<string, object>> kvParam = new List<KeyValuePair<string, object>>();
        foreach (var param in Parameters)
        {
            kvParam.Add(new KeyValuePair<string, object>(param.Key, param.Value));
        }
        return ExecuteScalar(query, kvParam.ToArray());
    }

    public object ExecuteScalar(string query, params KeyValuePair<string, object>[] Parameters)
    {
        using (SqlCommand comm = CreateCommand(query, Parameters))
        {
            return comm.ExecuteScalar();
        }
    }

    private class BatchStatement
    {
        public string Query;
        public KeyValuePair<string, object>[] Parameters;
    }

    List<BatchStatement> _batchStatements;
    public void StartBatch()
    {
        _batchStatements = new List<BatchStatement>();
    }
    public void AddBatchStatement(string query, SortedDictionary<string, object> Parameters)
    {
        List<KeyValuePair<string, object>> kvParam = new List<KeyValuePair<string, object>>();
        foreach (var param in Parameters)
        {
            kvParam.Add(new KeyValuePair<string, object>(param.Key, param.Value));
        }
        AddBatchStatement(query, kvParam.ToArray());
    }
    public void AddBatchStatement(string query, params KeyValuePair<string, object>[] parameters)
    {
        _batchStatements.Add(new BatchStatement() { Query = query, Parameters = parameters });
    }
    public void CommitBatch()
    {
        string query = string.Empty;
        Dictionary<string, object> parameters = new Dictionary<string, object>();
        int auxCount = 0;
        foreach (var entry in _batchStatements)
        {
            if (entry.Parameters != null)
            {
                foreach (var p in entry.Parameters)
                {
                    // if there is a duplicate parameter...
                    if (parameters.ContainsKey(p.Key))
                    {
                        entry.Query = entry.Query.Replace(p.Key + ",", p.Key + auxCount + ",")
                            .Replace(p.Key + " ", p.Key + auxCount + " ")
                            .Replace(p.Key + ")", p.Key + auxCount + ")");
                        parameters.Add(p.Key + auxCount, p.Value);
                    }
                    else
                    {
                        parameters.Add(p.Key, p.Value);
                    }
                }
            }
            query += entry.Query + ";" + Environment.NewLine;
            auxCount++;
        }
        if (auxCount > 0)
        {
            ExecuteSQL(query, parameters.ToArray());
        }
        RollBackBatch();
    }
    public void RollBackBatch()
    {
        _batchStatements.Clear();
        _batchStatements = null;
    }

    public DataSet GetDataSet(string query, SortedDictionary<string, object> Parameters)
    {
        List<KeyValuePair<string, object>> kvParam = new List<KeyValuePair<string, object>>();
        foreach (var param in Parameters)
        {
            kvParam.Add(new KeyValuePair<string, object>(param.Key, param.Value));
        }
        return GetDataSet(query, kvParam.ToArray());
    }

    public DataSet GetDataSet(string query, params KeyValuePair<string, object>[] Parameters)
    {
        DataSet oDataTable = new DataSet();

        using (SqlCommand comm = CreateCommand(query, Parameters))
        {
            SqlDataAdapter oDataAdapter = new SqlDataAdapter(comm);
            oDataAdapter.Fill(oDataTable);
            oDataAdapter.Dispose();

            return oDataTable;
        }
    }

    public DataTable GetDataTable(string query, SortedDictionary<string, object> Parameters)
    {
        List<KeyValuePair<string, object>> kvParam = new List<KeyValuePair<string, object>>();
        foreach (var param in Parameters)
        {
            kvParam.Add(new KeyValuePair<string, object>(param.Key, param.Value));
        }
        return GetDataTable(query, kvParam.ToArray());
    }

    public DataTable GetDataTable(string query, params KeyValuePair<string, object>[] Parameters)
    {
        //System.Diagnostics.Stopwatch sw = new System.Diagnostics.Stopwatch();
        //sw.Start();
        DataTable oDataTable = new DataTable();
        string cacheHash = null;
        bool useCache = query.StartsWith("#");

        if (useCache)
        {
            query = query.Substring(1);

            cacheHash = query;

            if (Parameters != null && Parameters.Length > 0)
            {
                foreach (var p in Parameters)
                {
                    cacheHash = cacheHash.Replace(("@" + p.Key).Replace("@@", "@"), p.Value.ToString());
                }
            }


            if (cachedQueries.ContainsKey(cacheHash)) return cachedQueries[cacheHash];
        }

        using (SqlCommand comm = CreateCommand(query, Parameters))
        {
            SqlDataAdapter oDataAdapter = new SqlDataAdapter(comm);
            oDataAdapter.Fill(oDataTable);
            oDataAdapter.Dispose();
            //System.IO.File.AppendAllText("c:\\temp\\risk-log.csv", "\"" + query.Replace(Environment.NewLine, " ").Replace("\"","¨") + "\"," + DateTime.Now.ToString("HH:mm:ss fff") +  "," + sw.ElapsedMilliseconds + Environment.NewLine);
            if (useCache)
            {
                cachedQueries.Add(cacheHash, oDataTable);
            }
            return oDataTable;
        }
    }


    private Dictionary<string, DataTable> cachedQueries = new Dictionary<string, DataTable>();

    public List<T> GetRecords<T>(DataTable dt) where T : ITable, new()
    {
        List<T> result = new List<T>();

        T newObject = new T();

        Dictionary<string, Type> Properties = GetObjectProperties<T>();

        for (int i = 0; i < Properties.Count; i++)
        {
            if (!dt.Columns.Contains(Properties.Keys.ElementAt(i)))
            {
                Properties.Remove(Properties.Keys.ElementAt(i));
                i--;
            }
        }

        var nType = newObject.GetType();

        foreach (DataRow row in dt.Rows)
        {
            newObject = new T();

            foreach (var prop in Properties)
            {
                var nProp = nType.GetProperty(prop.Key);
                if (((!object.ReferenceEquals(row[prop.Key], DBNull.Value))))
                {
                    if (nProp.PropertyType.IsValueType || nProp.PropertyType.Namespace == "System")
                    {
                        nProp.SetValue(newObject, row[prop.Key], null);
                    }
                    else
                    {
                        TypeConverter conv = TypeDescriptor.GetConverter(nProp.PropertyType);

                        if (conv.CanConvertFrom(row[prop.Key].GetType()))
                        {
                            nProp.SetValue(newObject, conv.ConvertFrom(row[prop.Key]), null);
                        }
                    }
                }
                else
                {
                    TypeConverter conv = TypeDescriptor.GetConverter(nProp.PropertyType);

                    if (conv.CanConvertFrom(row[prop.Key].GetType()))
                    {
                        nProp.SetValue(newObject, conv.ConvertFrom(row[prop.Key]), null);
                    }
                }
            }

            result.Add(newObject);
        }


        return result;
    }

    public List<T> GetRecords<T>(string query, params KeyValuePair<string, object>[] parameters) where T : ITable, new()
    {
        DataTable dt = GetDataTable(query, parameters);

        return GetRecords<T>(dt);
    }

    public List<T> GetRecords<T>(string[,] parameters) where T : ITable, new()
    {
        List<KeyValuePair<string, object>> kvParam = new List<KeyValuePair<string, object>>();
        for (var i = 0; i < parameters.Length; i++)
        {
            kvParam.Add(new KeyValuePair<string, object>(parameters[i, 0], parameters[i, 1]));
        }
        return GetRecords<T>(0, 0, null, kvParam.ToArray());
    }

    public List<T> GetRecords<T>(SortedDictionary<string, object> parameters) where T : ITable, new()
    {
        List<KeyValuePair<string, object>> kvParam = new List<KeyValuePair<string, object>>();
        foreach (var param in parameters)
        {
            kvParam.Add(new KeyValuePair<string, object>(param.Key, param.Value));
        }
        return GetRecords<T>(0, 0, null, kvParam.ToArray());
    }

    public List<T> GetRecords<T>(params KeyValuePair<string, object>[] parameters) where T : ITable, new()
    {
        return GetRecords<T>(0, 0, null, parameters);
    }

    public List<T> GetRecords<T>(int take = 0, int skip = 0, string orderColumn = null, params KeyValuePair<string, object>[] parameters) where T : ITable, new()
    {
        List<T> result = new List<T>();

        T newObject = new T();

        Dictionary<string, Type> Properties = GetObjectProperties<T>();

        List<string> where = new List<string>();
        foreach (var param in parameters)
        {
            if (param.Key.StartsWith("@"))
            {
                if (param.Value == null || param.Value == DBNull.Value)
                {
                    where.Add("T." + param.Key.Substring(1) + " is null");
                }
                else
                {
                    where.Add("T." + param.Key.Substring(1) + " = " + param.Key);
                }
            }
            else
            {
                if (param.Value.ToString().StartsWith("%") || param.Value.ToString().EndsWith("%"))
                {
                    where.Add("T." + param.Key + " like '" + param.Value.ToString() + "'");
                }
                else
                {
                    if (param.Value == null || param.Value == DBNull.Value)
                    {
                        where.Add("T." + param.Key + " is null");
                    }
                    else
                    {
                        where.Add("T." + param.Key + " = @" + param.Key);
                    }
                }
            }
        }


        string whereCondition = string.Empty;
        if (where.Count > 0)
        {
            whereCondition = " where " + string.Join(" and ", where);
        }

        // join conditions if any
        string joinConditions = string.Empty;
        string joinValues = string.Empty;
        int joinCount = 0;
        foreach (var join in Properties.Where(P => P.Value == typeof(LookupField) || P.Value == typeof(LookupObject)))
        {
            var currentProperty = newObject.GetType().GetProperty(join.Key);


            // if the property attribute for the table is present then it will be just a simple data type (string, double, int, datetime, etc...)
            // no need to instanciate the object just generate the join
            if (join.Value == typeof(LookupField))
            {
                var currentAttributes = ((LookupField)currentProperty.GetCustomAttribute(typeof(LookupField), false));

                joinConditions += " left join [" + currentAttributes.TableName + "] J" + joinCount + " on T.[" + currentAttributes.KeyField +
                    "] = J" + joinCount + ".[" + currentAttributes.TableKey + "]";

                joinValues += ", J" + joinCount + ".[" +
                    (string.IsNullOrEmpty(currentAttributes.LookupFieldName) ? join.Key : currentAttributes.LookupFieldName)
                    + "] as [J" + joinCount + "-" + join.Key + "]";

                joinCount++;
            }
            else
            {
                var currentAttributes = ((LookupObject)currentProperty.GetCustomAttribute(typeof(LookupObject), false));

                // the property may be null, so need to instanciate it first
                var newJoinObject = Activator.CreateInstance(newObject.GetType().GetProperty(join.Key).PropertyType);


                string tableName = ((ITable)newJoinObject).TableName;
                var fields = GetObjectProperties(currentProperty.PropertyType);

                joinConditions += " left join [" + tableName + "] J" + joinCount + " on T.[" + currentAttributes.KeyField
                    + "] = J" + joinCount + ".[" + fields.First(F => F.Value == typeof(PrimaryKeyDefinition)).Key + "] ";

                foreach (var field in fields.Where(F => (F.Value == null || F.Value == typeof(PrimaryKeyDefinition)) && F.Key != "TableName"))
                {
                    joinValues += ", J" + joinCount + ".[" + field.Key + "] as  [J" + joinCount + "-" + field.Key + "] ";
                }

                joinCount++;

                var tupla = generateJoins(ref fields, ref newJoinObject, ref joinCount);

                joinConditions += tupla.Item1;
                joinValues += tupla.Item2;

                currentProperty.SetValue(
                   newObject,
                   newJoinObject
               );
            }


        }

        var paramsNew = parameters.Where(P => P.Key.StartsWith("@") || !(P.Value.ToString().StartsWith("%") || P.Value.ToString().EndsWith("%")) && !P.Key.StartsWith("@"));

        StringBuilder query = new StringBuilder();
        query.AppendFormat("select {0} T.* {1} from {2} T ",
                (take > 0 && skip == 0 ? "top " + take : ""),
                joinValues,
                FormatTableName(newObject.TableName)
            );
        query.Append(joinConditions);
        query.Append(whereCondition);

        //string sSQL = "select " + (take > 0 && skip == 0 ? "top " + take : "") + " T.* " + joinValues + " from [" + newObject.TableName.Replace(".","].[") + "] T " + joinConditions + whereCondition;
        if (take > 0 && skip > 0 && !string.IsNullOrEmpty(orderColumn))
        {
            //sSQL += " ORDER BY [" + orderColumn + "] OFFSET " + skip + " ROWS FETCH NEXT " + take + " ROWS ONLY";
            query.Append(" ORDER BY [" + orderColumn + "] OFFSET " + skip + " ROWS FETCH NEXT " + take + " ROWS ONLY");
        }

        DataTable dt = GetDataTable(query.ToString(),
            paramsNew.ToArray());


        var columns = dt.Columns;
        foreach (DataRow row in dt.Rows)
        {
            newObject = new T();
            var type = newObject.GetType();
            joinCount = 0;

            foreach (var prop in Properties)
            {
                if (prop.Value == typeof(LookupField))
                {

                    string columnName = "J" + joinCount + "-" + prop.Key;
                    if (columns.Contains(columnName))
                    {
                        if (!object.ReferenceEquals(row[columnName], DBNull.Value))
                        {
                            type.GetProperty(prop.Key).SetValue(newObject, row[columnName], null);
                        }
                    }
                    joinCount++;
                }
                else if (prop.Value == typeof(LookupObject))
                {
                    // instanciate the object
                    var newProperty = Activator.CreateInstance(type.GetProperty(prop.Key).PropertyType);

                    // add the values
                    var currentProperty = newObject.GetType().GetProperty(prop.Key);
                    foreach (var cProp in GetObjectProperties(currentProperty.PropertyType))
                    {
                        string columnName = "J" + joinCount + "-" + cProp.Key;
                        if (columns.Contains(columnName))
                        {
                            if (!object.ReferenceEquals(row[columnName], DBNull.Value))
                            {
                                newProperty.GetType().GetProperty(cProp.Key).SetValue(
                                    newProperty,
                                    row[columnName], null
                                );
                            }
                        }
                    }

                    // assign the lookup for the current lookup?
                    joinCount++;

                    assignValues(ref newProperty, ref joinCount, ref columns, row);

                    // set the value in the parent object
                    type.GetProperty(prop.Key).SetValue(
                        newObject,
                        newProperty
                    );


                }
                else
                {
                    if (columns.Contains(prop.Key))
                    {
                        if (!object.ReferenceEquals(row[prop.Key], DBNull.Value))
                        {
                            type.GetProperty(prop.Key).SetValue(newObject, row[prop.Key], null);
                        }
                    }
                }
            }

            result.Add(newObject);
        }


        return result;
    }

    public int GetRowCount(string tableName)
    {
        string sSQL = @"SELECT SUM(PART.rows) AS rows
                            FROM sys.tables TBL
                            INNER JOIN sys.partitions PART ON TBL.object_id = PART.object_id
                            INNER JOIN sys.indexes IDX ON PART.object_id = IDX.object_id
                            AND PART.index_id = IDX.index_id
                            WHERE TBL.name = '" + tableName + "' AND IDX.index_id < 2 GROUP BY TBL.object_id, TBL.name";

        return Convert.ToInt32(ExecuteScalar(sSQL));
    }

    private void assignValues(ref object newObject, ref int joinCount, ref DataColumnCollection columns, DataRow row)
    {
        var type = newObject.GetType();
        Dictionary<string, Type> Properties = GetObjectProperties(type);

        foreach (var prop in Properties)
        {
            if (prop.Value == typeof(LookupField))
            {

                string columnName = "J" + joinCount + "-" + prop.Key;
                if (columns.Contains(columnName))
                {
                    if (!object.ReferenceEquals(row[columnName], DBNull.Value))
                    {
                        type.GetProperty(prop.Key).SetValue(newObject, row[columnName], null);
                    }
                }
                joinCount++;
            }
            else if (prop.Value == typeof(LookupObject))
            {
                // instanciate the object
                var newProperty = Activator.CreateInstance(type.GetProperty(prop.Key).PropertyType);

                // add the values
                var currentProperty = newObject.GetType().GetProperty(prop.Key);
                foreach (var cProp in GetObjectProperties(currentProperty.PropertyType))
                {
                    string columnName = "J" + joinCount + "-" + cProp.Key;
                    if (columns.Contains(columnName))
                    {
                        if (!object.ReferenceEquals(row[columnName], DBNull.Value))
                        {
                            newProperty.GetType().GetProperty(cProp.Key).SetValue(
                                newProperty,
                                row[columnName], null
                            );
                        }
                    }
                }

                // assign the lookup for the current lookup?

                joinCount++;

                assignValues(ref newProperty, ref joinCount, ref columns, row);

                // set the value in the parent object
                type.GetProperty(prop.Key).SetValue(
                    newObject,
                    newProperty
                );


            }
        }
    }

    private Tuple<string, string> generateJoins(ref Dictionary<string, Type> Properties, ref object newObject, ref int joinCount)
    {

        string joinConditions = string.Empty;
        string joinValues = string.Empty;
        int originalCount = joinCount - 1;

        foreach (var join in Properties.Where(P => P.Value == typeof(LookupField) || P.Value == typeof(LookupObject)))
        {
            var currentProperty = newObject.GetType().GetProperty(join.Key);


            // if the property attribute for the table is present then it will be just a simple data type (string, double, int, datetime, etc...)
            // no need to instanciate the object just generate the join
            if (join.Value == typeof(LookupField))
            {
                var currentAttributes = ((LookupField)currentProperty.GetCustomAttribute(typeof(LookupField), false));

                if (currentAttributes.ignoreIfChild) continue;

                joinConditions += " left join [" + currentAttributes.TableName + "] J" + joinCount + " on J" + originalCount + ".[" + currentAttributes.KeyField +
                    "] = J" + joinCount + ".[" + currentAttributes.TableKey + "]";

                joinValues += ", J" + joinCount + ".[" +
                    (string.IsNullOrEmpty(currentAttributes.LookupFieldName) ? join.Key : currentAttributes.LookupFieldName)
                    + "] as [J" + joinCount + "-" + join.Key + "]";

                joinCount++;
            }
            else
            {
                var currentAttributes = ((LookupObject)currentProperty.GetCustomAttribute(typeof(LookupObject), false));

                if (currentAttributes.ignoreIfChild) continue;

                // instanciate a new object
                object joinObject = Activator.CreateInstance(newObject.GetType().GetProperty(join.Key).PropertyType);

                string tableName = ((ITable)currentProperty.GetValue(newObject)).TableName;
                var fields = GetObjectProperties(currentProperty.PropertyType);
                joinConditions += " left join [" + tableName + "] J" + joinCount + " on J" + originalCount + ".[" + currentAttributes.KeyField
                    + "] = J" + joinCount + ".[" + fields.First(F => F.Value == typeof(PrimaryKeyDefinition)).Key + "] ";

                foreach (var field in fields.Where(F => (F.Value == null || F.Value == typeof(PrimaryKeyDefinition)) && F.Key != "TableName"))
                {
                    joinValues += ", J" + joinCount + ".[" + field.Key + "] as  [J" + joinCount + "-" + field.Key + "] ";
                }

                joinCount++;

                // TODO --> Recursive Joins to child Lookups... stop if the join is related to a parent table already joined
                var newjoins = generateJoins(ref fields, ref joinObject, ref joinCount);
                joinConditions += " " + newjoins.Item1;
                joinValues += " " + newjoins.Item2;

                // assign the new object to the property
                currentProperty.SetValue(
                    newObject,
                    joinObject
                );
            }

        }

        return new Tuple<string, string>(joinConditions, joinValues);
    }

    private Dictionary<string, Type> GetObjectProperties<T>() where T : ITable
    {
        Dictionary<string, Type> properties = new Dictionary<string, Type>();
        foreach (PropertyInfo info in typeof(T).GetProperties())
        {
            if (info.GetCustomAttribute(typeof(RelatedField), false) != null)
            {
                properties.Add(info.Name, typeof(RelatedField));
            }
            else if (info.GetCustomAttribute(typeof(LookupField), false) != null)
            {
                properties.Add(info.Name, typeof(LookupField));
            }
            else if (info.GetCustomAttribute(typeof(LookupObject), false) != null)
            {
                properties.Add(info.Name, typeof(LookupObject));
            }
            else if (info.GetCustomAttribute(typeof(PrimaryKeyDefinition), false) != null)
            {
                properties.Add(info.Name, typeof(PrimaryKeyDefinition));
            }
            else
            {
                properties.Add(info.Name, null);
            }
        }

        return properties;
    }

    private Dictionary<string, Type> GetObjectProperties(Type who)
    {
        Dictionary<string, Type> properties = new Dictionary<string, Type>();
        foreach (PropertyInfo info in who.GetProperties())
        {
            if (info.GetCustomAttribute(typeof(RelatedField), false) != null)
            {
                properties.Add(info.Name, typeof(RelatedField));
            }
            else if (info.GetCustomAttribute(typeof(LookupField), false) != null)
            {
                properties.Add(info.Name, typeof(LookupField));
            }
            else if (info.GetCustomAttribute(typeof(LookupObject), false) != null)
            {
                properties.Add(info.Name, typeof(LookupObject));
            }
            else if (info.GetCustomAttribute(typeof(PrimaryKeyDefinition), false) != null)
            {
                properties.Add(info.Name, typeof(PrimaryKeyDefinition));
            }
            else
            {
                properties.Add(info.Name, null);
            }
        }

        return properties;
    }

    private Dictionary<string, bool> GetITablePK<T>() where T : ITable
    {
        Dictionary<string, bool> properties = new Dictionary<string, bool>();
        foreach (PropertyInfo info in typeof(T).GetProperties())
        {
            var attr = info.GetCustomAttribute(typeof(PrimaryKeyDefinition), false);

            if (attr != null)
            {
                properties.Add(info.Name, ((PrimaryKeyDefinition)attr).AutoNumeric);
            }
        }

        return properties;
    }

    public static string FormatTableName(string name)
    {
        name = name.Trim();
        if (!name.StartsWith("[")) name = "[" + name;
        if (!name.EndsWith("]")) name = name + "]";
        name = name.Replace(".", "].[").Replace("]]", "]").Replace("[[", "[");
        return name;
    }

    public void InsertDataTableSQL(DataTable dt, string schema = null)
    {
        string sSQL = "";
        string values = "";
        foreach (DataColumn column in dt.Columns)
        {

            sSQL += ",[" + column.ColumnName + "]";
            values += ",@param" + column.Ordinal;
        }
        sSQL = "insert into " + (string.IsNullOrEmpty(schema) ? "" : schema + ".") + "[" + dt.TableName + "] (" + sSQL.Substring(1) + ") values (" + values.Substring(1) + ")";

        foreach (DataRow row in dt.Rows)
        {
            List<KeyValuePair<string, object>> parameters = new List<KeyValuePair<string, object>>();
            foreach (DataColumn column in dt.Columns)
            {
                parameters.Add(new KeyValuePair<string, object>("@param" + column.Ordinal, row[column]));

            }

            ExecuteScalar(sSQL, parameters.ToArray());
        }
    }

    public void InsertDataTable(DataTable dt, string schema = null)
    {

        using (SqlBulkCopy s = new SqlBulkCopy(_conn))
        {
            s.DestinationTableName = (string.IsNullOrEmpty(schema) ? (string.IsNullOrEmpty(dt.Namespace) ? "" : "[" + dt.Namespace + "].") : "[" + schema + "].") + "[" + dt.TableName + "]";


            foreach (var column in dt.Columns) s.ColumnMappings.Add(column.ToString(), column.ToString());

            s.BulkCopyTimeout = 60; // 1 minute
            s.BatchSize = 5000;

            s.WriteToServer(dt);
        }
    }

    public List<TableDetails> FindRelationships(string table, string schema)
    {
        string sSQL = @"inner join (SELECT
FK.TABLE_NAME as FK_Table,
CU.COLUMN_NAME as FK_Column,
FK.TABLE_SCHEMA as FK_Schema,
PK.TABLE_NAME as PK_Table,
PT.COLUMN_NAME as PK_Column,
Constraint_Name = C.CONSTRAINT_NAME
FROM INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS C
INNER JOIN INFORMATION_SCHEMA.TABLE_CONSTRAINTS FK ON C.CONSTRAINT_NAME = FK.CONSTRAINT_NAME
INNER JOIN INFORMATION_SCHEMA.TABLE_CONSTRAINTS PK ON C.UNIQUE_CONSTRAINT_NAME = PK.CONSTRAINT_NAME
INNER JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE CU ON C.CONSTRAINT_NAME = CU.CONSTRAINT_NAME
INNER JOIN (
SELECT i1.TABLE_NAME, i2.COLUMN_NAME
FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS i1
INNER JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE i2 ON i1.CONSTRAINT_NAME = i2.CONSTRAINT_NAME
WHERE i1.CONSTRAINT_TYPE = 'PRIMARY KEY'
) PT ON PT.TABLE_NAME = PK.TABLE_NAME
WHERE FK.TABLE_NAME = '" + table.Replace("'", "''") + "'";
        if (!string.IsNullOrEmpty(schema))
        {
            sSQL += " and FK.TABLE_SCHEMA = '" + schema.Replace("'", "''") + "'";
        }
        sSQL += ") E on E.PK_Table = t.name and E.FK_Schema = OBJECT_SCHEMA_NAME(t.object_id)";

        List<TableDetails> details = new List<TableDetails>();

        // list all tables:
        foreach (DataRow r in GetDataTable("SELECT distinct OBJECT_SCHEMA_NAME(t.object_id) AS schema_name, E.FK_COlumn ,t.name AS table_name ,p.rows AS rows, E.FK_Column  AS rows FROM sys.tables t INNER JOIN sys.indexes i ON t.object_id = i.object_id INNER JOIN sys.partitions p ON i.object_id=p.object_id AND i.index_id=p.index_id " + sSQL).Rows)
        {
            var det = new TableDetails()
            {
                Name = r["TABLE_NAME"].ToString()
                ,
                Schema = r["schema_name"].ToString()
                ,
                Type = "TABLE"
                ,
                RowCount = long.Parse("0" + r["rows"].ToString().Replace(",", "").Replace(".", ""))
                ,
                FileGroup = r["FK_Column"].ToString()

            };
            if (details.Any(D => D.Name == det.Name && D.Schema == det.Schema)) continue;

            details.Add(det);
        }

        return details;
    }

    public List<TableDetails> TableDetails(bool loadExtendedDetails = true)
    {
        List<TableDetails> details = new List<TableDetails>();

        string sSQL = @"SELECT case when Z.TABLE_NAME is null then 0 else 1 end as [HasPK], OBJECT_SCHEMA_NAME(t.object_id) AS schema_name ,t.name AS table_name ,i.index_id ,i.name AS index_name ,p.partition_number ,fg.name AS filegroup_name ,p.rows AS rows 
FROM sys.tables t INNER JOIN sys.indexes i ON t.object_id = i.object_id INNER JOIN sys.partitions p ON i.object_id=p.object_id AND i.index_id=p.index_id LEFT OUTER JOIN sys.partition_schemes ps ON i.data_space_id=ps.data_space_id LEFT OUTER JOIN sys.destination_data_spaces dds ON ps.data_space_id=dds.partition_scheme_id AND p.partition_number=dds.destination_id INNER JOIN sys.filegroups fg ON COALESCE(dds.data_space_id, i.data_space_id)=fg.data_space_id
outer apply (
select top 1 C.Table_Name, C.Table_Schema
from INFORMATION_SCHEMA.COLUMNS C
left join information_schema.key_column_usage K on K.Table_name = C.Table_Name and K.Table_Schema = C.Table_Schema and K.Column_Name = C.Column_Name 
where  C.TABLE_NAME = t.name and C.TABLE_SCHEMA = OBJECT_SCHEMA_NAME(t.object_id) and not  OBJECTPROPERTY(OBJECT_ID(CONSTRAINT_SCHEMA + '.' + CONSTRAINT_NAME), 'IsPrimaryKey') is null
) Z";


        // list all tables:
        foreach (DataRow r in GetDataTable(sSQL).Rows)
        {
            var det = new TableDetails()
            {
                Name = r["TABLE_NAME"].ToString()
                ,
                Schema = r["schema_name"].ToString()
                ,
                Type = "TABLE"
                ,
                FileGroup = r["filegroup_name"].ToString()
                ,
                RowCount = long.Parse("0" + r["rows"].ToString().Replace(",", "").Replace(".", ""))
                ,
                HasPk = Convert.ToBoolean(r["HasPk"])
            };
            if (details.Any(D => D.Name == det.Name && D.Schema == det.Schema)) continue;

            if (loadExtendedDetails)
            {
                DataRow d = GetDataTable("sp_spaceused '" + det.Schema + "." + det.Name + "'").Rows[0];

                det.IndexSpace = double.Parse(d["index_size"].ToString().Replace(" KB", "").Replace(",", ""));
                det.DiskSpace = double.Parse(d["data"].ToString().Replace(" KB", "").Replace(",", ""));
                det.UnusedSpace = double.Parse(d["unused"].ToString().Replace(" KB", "").Replace(",", ""));
                det.Reserved = double.Parse(d["Reserved"].ToString().Replace(" KB", "").Replace(",", ""));

            }
            details.Add(det);
        }

        return details;
    }

    public List<ColumnDetails> ColumnDetails(string TableName, string Schema = "dbo", bool CheckDuplicates = false)
    {

        if (TableName.Contains("."))
        {
            Schema = TableName.Split('.')[0];
            TableName = TableName.Split('.')[1];
        }

        List<ColumnDetails> details = new List<ColumnDetails>();

        foreach (DataRow r in GetDataTable("select K.ORDINAL_POSITION as [Key], COLUMNPROPERTY(object_id(C.TABLE_SCHEMA + '.' + C.TABLE_NAME), C.COLUMN_NAME, 'IsIdentity') as [IDENTITY], C.COLUMN_NAME, C.DATA_TYPE, C.IS_NULLABLE, OBJECTPROPERTY(OBJECT_ID(CONSTRAINT_SCHEMA + '.' + CONSTRAINT_NAME), 'IsPrimaryKey') as [IS_PK] " +
            " from INFORMATION_SCHEMA.COLUMNS C " +
            " left join information_schema.key_column_usage K on K.Table_name = C.Table_Name and K.Table_Schema = C.Table_Schema and K.Column_Name = C.Column_Name " +
            " where C.TABLE_NAME = @tableName and C.TABLE_SCHEMA = @schema ",
            new SortedDictionary<string, object>()
            {
                    {"@tableName", TableName},
                    {"@schema", Schema}
            }
            ).Rows)
        {
            var column = new ColumnDetails()
            {
                Key = (r["KEY"] != DBNull.Value && (int)r["key"] > 0),
                DataType = r["DATA_TYPE"].ToString(),
                Name = r["COLUMN_NAME"].ToString(),
                isNullable = r["IS_NULLABLE"].ToString() == "YES",
                autoincrement = (r["IDENTITY"] != DBNull.Value && r["IDENTITY"].ToString() == "1"),
                hasDuplicates = false,
                IsPk = Convert.ToBoolean(r["IS_PK"] == DBNull.Value ? false : r["IS_PK"])
            };

            if (CheckDuplicates)
            {
                // case sensitive grouping for text fields
                object dups;
                if (column.DataType.ToLower().Contains("char") || column.DataType.ToLower().Contains("text"))
                {
                    dups = ExecuteScalar("select top 1 [" + column.Name +
                    "] COLLATE SQL_Latin1_General_CP1_CS_AS from [" + Schema + "].[" + TableName + "]  group by [" + column.Name +
                    "] COLLATE SQL_Latin1_General_CP1_CS_AS having count([" + column.Name + "]) > 1");
                }
                else
                {
                    dups = ExecuteScalar("select top 1 [" + column.Name +
                    "] from [" + Schema + "].[" + TableName + "]  group by [" + column.Name +
                    "] having count([" + column.Name + "]) > 1");
                }
                column.hasDuplicates = dups != null && dups != DBNull.Value;
            }

            details.Add(column);
        }

        return details;
    }

    public List<TableDetails> ViewDetails(bool count = true)
    {
        List<TableDetails> details = new List<TableDetails>();

        foreach (DataRow r in GetDataTable("SELECT SCHEMA_NAME(schema_id) AS schema_name,name AS view_name FROM sys.views where is_ms_shipped = 0").Rows)
        {
            var det = new TableDetails()
            {
                Name = r["view_name"].ToString()
                ,
                Schema = r["schema_name"].ToString()
                ,
                Type = "VIEW"
                ,
                FileGroup = string.Empty
                ,
                RowCount = 0

            };
            if (count)
            {
                try
                {
                    det.RowCount = Convert.ToInt64(ExecuteScalar("select count(1) from [" + r["schema_name"].ToString() + "].[" + r["view_name"].ToString() + "]"));
                }
                catch
                {
                    //there is an issue with the view, so skipt it...
                }
            }
            if (details.Any(D => D.Name == det.Name && D.Schema == det.Schema)) continue;


            det.IndexSpace = 0;
            det.DiskSpace = 0;
            det.UnusedSpace = 0;
            det.Reserved = 0;

            details.Add(det);
        }


        return details;
    }
}

public class TableDetails
{
    public string Type;
    public string Name;
    public string Schema;
    public string FileGroup;
    public long RowCount;
    public double IndexSpace;
    public double DiskSpace;
    public double UnusedSpace;
    public double Reserved;
    public bool HasPk;
    //for the schemas
    public int ItemCount;
}

public class ColumnDetails
{
    public string Name;
    public string DataType;
    public bool isNullable;
    public bool autoincrement;
    public bool Key;
    public bool hasDuplicates;
    public bool IsPk;
}

[AttributeUsage(AttributeTargets.Property)]
public class PrimaryKeyDefinition : System.Attribute
{
    public bool IsPrimaryKey { get; set; }
    public bool AutoNumeric { get; set; }

    public PrimaryKeyDefinition(bool IsPrimaryKey, bool AutoNumeric = true)
    {
        this.IsPrimaryKey = IsPrimaryKey;
        this.AutoNumeric = AutoNumeric;
    }

}

[AttributeUsage(AttributeTargets.Property)]
public class LookupField : System.Attribute
{
    public string KeyField { get; set; }
    public string TableName { get; set; }
    public string TableKey { get; set; }
    public string LookupFieldName { get; set; }
    public bool ignoreIfChild { get; set; }
    public LookupField(string KeyField, string TableName = null, string TableKey = null, string LookupFieldName = null, bool ignoreIfChild = false)
    {
        this.KeyField = KeyField;
        this.TableName = TableName;
        this.TableKey = TableKey;
        this.LookupFieldName = LookupFieldName;
        this.ignoreIfChild = ignoreIfChild;
    }
}

[AttributeUsage(AttributeTargets.Property)]
public class LookupObject : System.Attribute
{
    public string KeyField { get; set; }
    public bool ignoreIfChild { get; set; }
    public LookupObject(string KeyField, bool ignoreIfChild = false)
    {
        this.KeyField = KeyField;
        this.ignoreIfChild = ignoreIfChild;
    }
}

[AttributeUsage(AttributeTargets.Property)]
public class RelatedField : System.Attribute
{
    public RelatedField()
    {
    }
}

public interface ITable
{
    string TableName { get; }
}

public interface IInstanceId
{
    long InstanceId { get; set; }
}

public interface IChangesetId
{
    int ChangesetId { get; set; }
}

public interface IObjectDefinitionId
{
    int ObjectDefinitionId { get; set; }
}

public class schema_table : ITable
{
    public string TableName => "INFORMATION_SCHEMA.Tables";

    public string TABLE_NAME { get; set; }
}