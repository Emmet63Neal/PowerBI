using Microsoft.IdentityModel.Clients.ActiveDirectory;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;

namespace Test0365AuditLogs
{
    class Program
    {
        static void Main(string[] args)
        {
            //string connstring = "Data Source=pbitelemetry.database.windows.net;Initial Catalog=PowerBITelemetry;Persist Security Info=True;User ID=pbi-admin;Password=c@t5rc00l";

            string connstring = "Server=tcp:modb1.database.windows.net,1433;Initial Catalog=test;Persist Security Info=False;User ID=pbiadmin;Password=Corp123!;MultipleActiveResultSets=False;Encrypt=True;TrustServerCertificate=False;Connection Timeout=30;";
            string schema = "[adtlog]";
            string clientSecret = "xERf7aWezdK0krhr8V/yAmP1Xh/xgpQs1NRKofwz3NI=";
            string tenant = "4fd354cb-e69d-49f6-b954-daf93d5cef95";
            string clientId = "caeb0317-a2ad-4842-9f87-a0c4c22aabe6";

            for(int i=0; i< 20; i++)
            {
                DateTime dateToProcess = DateTime.UtcNow.AddDays(-1 * i);
                var result = O365ETL.GetOfficeData.Process(clientId, clientSecret, tenant, dateToProcess, connstring, schema).Result;
            }

            O365ETL.SQLOperations.RunStoredProc(connstring, schema + ".uspMoveStagingToAuditLog");
        }
    }
}
