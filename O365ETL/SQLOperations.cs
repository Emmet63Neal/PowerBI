using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Odbc;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace O365ETL
{
    public class SQLOperations
    {

		public static void CreateTABLE(string connectionString, string schema, string tableName, DataTable table)
		{
			string sqlsc;
			sqlsc = $"CREATE TABLE {schema}.{tableName}(";
			for (int i = 0; i < table.Columns.Count; i++)
			{
				sqlsc += "\n [" + table.Columns[i].ColumnName + "] ";
				string columnType = table.Columns[i].DataType.ToString();
				switch (columnType)
				{
					case "System.Int32":
						sqlsc += " int ";
						break;
					case "System.Int64":
						sqlsc += " bigint ";
						break;
					case "System.Int16":
						sqlsc += " smallint";
						break;
					case "System.Byte":
						sqlsc += " tinyint";
						break;
					case "System.Decimal":
						sqlsc += " decimal ";
						break;
					case "System.DateTime":
						sqlsc += " datetime ";
						break;
					case "System.String":
					default:
						sqlsc += " nvarchar(max) ";
						break;
				}
				if (table.Columns[i].AutoIncrement)
					sqlsc += " IDENTITY(" + table.Columns[i].AutoIncrementSeed.ToString() + "," + table.Columns[i].AutoIncrementStep.ToString() + ") ";
				if (!table.Columns[i].AllowDBNull)
					sqlsc += " NOT NULL ";
				sqlsc += ",";
			}
			var sql = sqlsc.Substring(0, sqlsc.Length - 1) + "\n)";
			using (var connection = new SqlConnection(connectionString))
			{
				connection.Open();
				using (var cmd = connection.CreateCommand())
				{
					cmd.CommandText = sql;
					cmd.ExecuteNonQuery();
				}
			}
		}

	    public static bool TableExists(string connectionString, string tableName)
	    {
			bool exists;

		    try
		    {
			    using (var sqlConnection = new SqlConnection(connectionString))
			    {
					sqlConnection.Open();
				    SqlCommand cmd = sqlConnection.CreateCommand();
				    cmd.CommandText = "select case when exists((select * from information_schema.tables where table_name = '" +
				                      tableName +
				                      "')) then 1 else 0 end";
				    exists = (int) cmd.ExecuteScalar() == 1;
			    }
		    }
		    catch
		    {
			    exists = false;
		    }
		    return exists;
	    }

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

	    public static void InsertAuditLog(List<DataTable> dataTables, string connstring, string schema)
	    {
			foreach (DataTable dataTable in dataTables)
			{
				var stagingTableName = $"staging_{dataTable.TableName}";
				var productionTableName = dataTable.TableName;
				if(!SQLOperations.TableExists(connstring, stagingTableName))
				{
					SQLOperations.CreateTABLE(connstring, schema, stagingTableName, dataTable);
				}
				if (!SQLOperations.TableExists(connstring, productionTableName))
				{
					SQLOperations.CreateTABLE(connstring, schema, productionTableName, dataTable);
				}
				BulkInsert(connstring, dataTable, $"{schema}.{stagingTableName}");
			}
		}
    }   
}
