using Microsoft.IdentityModel.Clients.ActiveDirectory;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Net.Http;
using System.Threading.Tasks;

namespace O365ETL
{
    public class GetOfficeData
    {
        public static async Task GetContentPayloadAsync(string accessToken, string tenantId, DateTime start, string connstring, string schema)
        {
            DateTime end = start.AddDays(1);

            HttpClient client = new HttpClient();
            client.DefaultRequestHeaders.Authorization = new System.Net.Http.Headers.AuthenticationHeaderValue("Bearer", accessToken);

            string pageUri = $"https://manage.office.com/api/v1.0/{tenantId}/activity/feed/subscriptions/content?contentType=Audit.General&startTime={start.Date}&endTime={end.Date}";
            do
            {
                HttpResponseMessage batchResponse = await client.GetAsync(pageUri);
                var payloadRetrieved = await batchResponse.Content.ReadAsStringAsync();

                if (!batchResponse.IsSuccessStatusCode)
                {
                    throw new ApiFailure()
                    {
                        MainPayload = payloadRetrieved,
                        RawResponse = batchResponse,
                    };
                }

                var contentReturned = JsonConvert.DeserializeObject<List<Content>>(payloadRetrieved);
                IEnumerable<String> values;
                batchResponse.Headers.TryGetValues("nextpageUri", out values);
                if (values != null)
                {
                    pageUri = (values as string[]).FirstOrDefault();
                }
                else
                {
                    pageUri = null;
                }

                foreach (var content in contentReturned)
                {
                    var dataResponse = await client.GetAsync(content.contentUri);
                    var dataResponsePayload = await dataResponse.Content.ReadAsStringAsync();

                    if (!dataResponse.IsSuccessStatusCode)// check if the httpclient success or not
                    {

                        var apiFailure = new ApiFailure()
                        {
                            MainPayload = payloadRetrieved,
                            RawResponse = batchResponse,
                            DataPayload = dataResponsePayload,
                        };
                        // We will not fail if the content payload his an error
                        Log(apiFailure, connstring, schema, start);
                    }
                    else
                    {
                        var contentReturnedAuditLog = JsonConvert.DeserializeObject<List<AuditLogJson>>(dataResponsePayload);
                        content.RecordCount = O365ETL.SQLOperations.InsertAuditLog(contentReturnedAuditLog, connstring, schema, start.ToString("yyyyMMddHHmmss"), dataResponsePayload);
                    }
                }

            } while (pageUri != null); // check if page is null then quit the loop
        }

        public static async Task<bool> Process(string clientId, string clientSecret, string tenant, DateTime date, string connstring, string schema)
        {
            try
            {
                var token = await GetToken(clientId, clientSecret, tenant);
                await O365ETL.GetOfficeData.GetContentPayloadAsync(token.AccessToken, tenant, date, connstring, schema);
                Log(null, connstring, schema, date);
            }
            catch (Exception exception)
            {
                Log(exception, connstring, schema, date);
                throw;
            }

            return true;
        }

        public static void Log(Exception exception, string connstring, string schema, DateTime date)
        {
            var errorDataTable = O365ETL.DataTables.GetErrorDataTable();
            DataRow errorRow = errorDataTable.NewRow();

            errorRow["BatchID"] = date.ToString("yyyyMMddHHmmss"); ;
            errorRow["LogText"] = exception?.ToString();
            errorRow["ExecutedBy"] = Environment.UserName;
            errorRow["RecordCount"] = 0;
            errorRow["EventStart"] = date;
            errorRow["EventEnd"] = DateTime.UtcNow;
            errorRow["Status"] = exception == null ? "Success" : "Fail";
            if (exception != null && exception is ApiFailure)
            {
                ApiFailure apiFailure = exception as ApiFailure;
                errorRow["BatchResponse"] = apiFailure.MainPayload;
                errorRow["IndividualResponse"] = apiFailure.DataPayload;
                errorRow["APIContentUri"] = apiFailure.RawResponse.RequestMessage.RequestUri;
            }
            errorRow["APIStartDate"] = date;
            errorRow["APIEndDate"] = date.AddDays(1);
            errorDataTable.Rows.Add(errorRow);
            O365ETL.SQLOperations.BulkInsert(connstring, errorDataTable, schema + "." + "batch_log");
        }

        public static async Task<AuthenticationResult> GetToken(string clientId, string clientSecret, string tenant)
        {
            string resourceUri = "https://manage.office.com";
            string authorityUri = $"https://login.windows.net/{tenant}/oauth2/authorize";

            AuthenticationContext authContext = new AuthenticationContext(authorityUri);
            ClientCredential cred = new ClientCredential(clientId, clientSecret);
            AuthenticationResult token = await authContext.AcquireTokenAsync(resourceUri, cred);
            return token;
        }

    }

    public class Content
    {
        public string contentCreated { get; set; }

        public string contentExpiration { get; set; }

        public string contentId { get; set; }

        public string contentType { get; set; }

        public string contentUri { get; set; }

        public string contentRawPayload { get; set; }

        public string BatchResponse { get; set; }

        public string IndividualResponse { get; set; }

        public int RecordCount { get; set; }


    }

    public class Dataset
    {
        public string DatasetId { get; set; }
        public string DatasetName { get; set; }
    }

    public class AuditLogJson
    {
        public string Id { get; set; }
        public int RecordType { get; set; }
        public string CreationTime { get; set; }
        public int UserType { get; set; }
        public string UserKey { get; set; }
        public List<Dataset> Datasets { get; set; }
    }

    public class ApiFailure : Exception
    {
        public string MainPayload { get; set; }
        public string DataPayload { get; set; }
        public HttpResponseMessage RawResponse { get; set; }
    }
}
