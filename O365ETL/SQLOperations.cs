using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace O365ETL
{
    public class SQLOperations
    {
        public static void BulkInsert(string connString, DataTable table, string tableName)
        {
            try
            {
                using (SqlBulkCopy bulk = new SqlBulkCopy(connString))
                {
                    bulk.BatchSize = 1000;
                    bulk.DestinationTableName = tableName;
                    bulk.WriteToServer(table);
                    bulk.Close();
                }
            }
            catch (Exception e)
            {
                throw new Exception("overflow during batch insert in table " + tableName);
            }
        }

        public static void RunStoredProc(string connstring, string storedProcedure)
        {
            using (SqlConnection con = new SqlConnection(connstring))
            {
                con.Open();
                using (SqlCommand cmd = new SqlCommand(storedProcedure, con))
                {
                    cmd.CommandType = CommandType.StoredProcedure;
                    cmd.ExecuteNonQuery();
                }
                con.Close();
            }
        }

        public static int InsertAuditLog(List<AuditLogJson> contentReturnedAuditLog, string connstring, string schema, string date, string response2Payload)
        {
            int count = 0;
            var AuditLogDataTable = O365ETL.DataTables.GetAuditLogDataTable();

            foreach (var data in contentReturnedAuditLog)
            {
                count++;
                DataRow AuditLogRow = AuditLogDataTable.NewRow();

                AuditLogRow["Id"] = data.Id ?? "";
                AuditLogRow["RecordType"] = data.RecordType;
                AuditLogRow["CreationTime"] = data.CreationTime ?? "";
                AuditLogRow["UserType"] = data.UserType;
                AuditLogRow["UserKey"] = data.UserKey ?? "";
                AuditLogRow["JSONPayload"] = response2Payload;
                AuditLogRow["BatchId"] = date;

                if (data.Datasets != null)
                {
                    var DataSetTable = O365ETL.DataTables.GetDatasetDataTable();
                    foreach (var data1 in data.Datasets)
                    {
                        DataRow AuditLogRowDataSet = DataSetTable.NewRow();
                        AuditLogRowDataSet["DatasetId"] = data1.DatasetId ?? "";
                        AuditLogRowDataSet["DatasetName"] = data1.DatasetName ?? "";
                        DataSetTable.Rows.Add(AuditLogRowDataSet);
                    }
                    
                    O365ETL.SQLOperations.BulkInsert(connstring, DataSetTable, schema + "." + "[staging_datasets]");
                }

                AuditLogDataTable.Rows.Add(AuditLogRow);
            }

            O365ETL.SQLOperations.BulkInsert(connstring, AuditLogDataTable, schema + "." + "[staging_audit_data]");

            return count;
        }
    }   
}
