using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace O365ETL
{
    class DataTables
    {
        public static DataTable GetErrorDataTable()
        {
            DataTable table = new DataTable();
            table.Columns.Add("BatchID");
            table.Columns.Add("LogText");
            table.Columns.Add("ExecutedBy");
            table.Columns.Add("RecordCount");
            table.Columns.Add("EventStart", typeof(DateTime));
            table.Columns.Add("EventEnd", typeof(DateTime));
            table.Columns.Add("Status");
            table.Columns.Add("BatchResponse");
            table.Columns.Add("IndividualResponse");
            table.Columns.Add("APIContentUri");
            table.Columns.Add("APIStartDate", typeof(DateTime));
            table.Columns.Add("APIEndDate", typeof(DateTime));

            return table;
        }
        public static DataTable GetDatasetDataTable()
        {
            DataTable table = new DataTable();
            table.Columns.Add("DatasetId");
            table.Columns.Add("DatasetName");
            return table;
        }
        public static DataTable GetAuditLogDataTable()
        {
            DataTable table = new DataTable();
            table.Columns.Add("Id");
            table.Columns.Add("RecordType");
            table.Columns.Add("CreationTime", typeof(DateTime));
            table.Columns.Add("UserType");
            table.Columns.Add("UserKey");
            table.Columns.Add("JSONPayload");
            table.Columns.Add("BatchId");
            return table;
        }
    }
}
