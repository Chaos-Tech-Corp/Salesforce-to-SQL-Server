using Salesforce.Common;
using Salesforce.Force;
using System;
using System.Collections.Generic;
using System.Net;
using System.Text;
using System.Threading.Tasks;

class SalesforceClient
{
    public string InstanceUrl;
    public string AccessToken;
    public string ApiVersion;
    public bool SessionIniciated;

    public ForceClient Client;

    // Connect App connection details
    private protected readonly string _ConsumerKey = "{Consumer-Key}";
    private protected readonly string _ConsumerSecret = "{Consumer-Secret}";
    // Salesforce User credentials
    private protected readonly string _Username = "{Username}";
    private protected readonly string _Password = "{User-Password}";
    private protected readonly string _SecurityToken = "{User-Security-Toke}";
    private protected readonly Boolean _IsSandbox = false;

    public SalesforceClient(string key, string secret, string userName, string password, string token, Boolean isSandbox, string ApiVersion = "v44.0")
    {
        _ConsumerKey = key;
        _ConsumerSecret = secret;
        _Username = userName;
        _Password = password;
        _SecurityToken = token;
        _IsSandbox = isSandbox;
        this.ApiVersion = ApiVersion;
    }

    public async System.Threading.Tasks.Task<bool> SalesforceLoginAsync()
    {
        System.Net.ServicePointManager.SecurityProtocol = SecurityProtocolType.Tls12;

        var auth = new AuthenticationClient();
        auth.ApiVersion = ApiVersion;
        var url = _IsSandbox ? "https://test.salesforce.com/services/oauth2/token"
                        : "https://login.salesforce.com/services/oauth2/token";

        await auth.UsernamePasswordAsync(_ConsumerKey, _ConsumerSecret, _Username, _Password + _SecurityToken, url);

        if (!string.IsNullOrEmpty(auth.InstanceUrl))
        {
            InstanceUrl = auth.InstanceUrl;
            AccessToken = auth.AccessToken;
            ApiVersion = auth.ApiVersion;
            SessionIniciated = true;
        }
        else
        {
            SessionIniciated = false;
        }


        return SessionIniciated;
    }

    public async System.Threading.Tasks.Task CreateClientAsync()
    {
        if (!SessionIniciated)
        {
            await SalesforceLoginAsync();
            Client = new ForceClient(InstanceUrl, AccessToken, ApiVersion);
        }

    }

    public async Task<List<dynamic>> QueryRecords(string query)
    {
        return await QueryRecords<dynamic>(query);
    }


    public async Task<List<T>> QueryRecords<T>(string query)
    {
        List<T> recordList = new List<T>();
        var recordQuery = await Client.QueryAsync<T>(query);
        recordList.AddRange(recordQuery.Records);
        while (!string.IsNullOrEmpty(recordQuery.NextRecordsUrl))
        {
            //add a small delay
            await Task.Delay(100);
            recordQuery = await Client.QueryContinuationAsync<T>(recordQuery.NextRecordsUrl);
            recordList.AddRange(recordQuery.Records);
        }
        return recordList;
    }


    async Task<Tuple<List<dynamic>, string>> QueryRecordsPaged(string query, bool queryAll = false)
    {
        List<dynamic> recordList = new List<dynamic>();

        Salesforce.Common.Models.Json.QueryResult<dynamic> queryRecordResult = queryAll ?
                        await Client.QueryAllAsync<dynamic>(query) :
                        await Client.QueryAsync<dynamic>(query);

        recordList.AddRange(queryRecordResult.Records);
        return new Tuple<List<dynamic>, string>(recordList, queryRecordResult.NextRecordsUrl);
    }

    async Task<Tuple<List<dynamic>, string>> QueryRecordsContinuation(string nextRecordsUrl)
    {
        List<dynamic> recordList = new List<dynamic>();

        Salesforce.Common.Models.Json.QueryResult<dynamic> queryRecordResult = await Client.QueryContinuationAsync<dynamic>(nextRecordsUrl);

        recordList.AddRange(queryRecordResult.Records);

        return new Tuple<List<dynamic>, string>(recordList, queryRecordResult.NextRecordsUrl);
    }

    public async Task<Tuple<List<dynamic>, string>> GetRecordsPaged(string query, string nextRecordsUrl, bool queryAll = false)
    {
        List<dynamic> eAccounts = new List<dynamic>();

        Salesforce.Common.Models.Json.QueryResult<dynamic> existing_accounts;
        if (queryAll)
        {
            existing_accounts = !string.IsNullOrEmpty(nextRecordsUrl) ?
            await Client.QueryContinuationAsync<dynamic>(nextRecordsUrl) :
            await Client.QueryAllAsync<dynamic>(query);
        }
        else
        {
            existing_accounts = !string.IsNullOrEmpty(nextRecordsUrl) ?
            await Client.QueryContinuationAsync<dynamic>(nextRecordsUrl) :
            await Client.QueryAsync<dynamic>(query);
        }

        eAccounts.AddRange(existing_accounts.Records);
        return new Tuple<List<dynamic>, string>(eAccounts, existing_accounts.NextRecordsUrl);
    }

}