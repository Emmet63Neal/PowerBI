using System;
using System.Collections.Generic;
using System.Data;
using System.Dynamic;
using System.Linq;
using System.Linq.Expressions;
using Newtonsoft.Json;
namespace ConsoleApplication4
{
	class Program
	{
		static void Main(string[] args)
		{
			string theString =
				"[{\"Id\":\"f555fe4f-dbe3-6ae5-3daa-8aa3692d6a00\",\"RecordType\":20,\"CreationTime\":\"2017-09-25T21:15:44\",\"Operation\":\"ViewDashboard\",\"OrganizationId\":\"4fd354cb-e69d-49f6-b954-daf93d5cef95\",\"UserType\":0,\"UserKey\":\"1003BFFDA4AB4F51\",\"Workload\":\"PowerBI\",\"UserId\":\"Nick@nealanalytics.com\",\"ClientIP\":\"67.183.153.248\",\"UserAgent\":\"Mozilla\\/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit\\/537.36 (KHTML\",\"Activity\":\"ViewDashboard\",\"ItemName\":\"Partner Center Analytics\",\"WorkSpaceName\":\"My Workspace\",\"DashboardName\":\"Partner Center Analytics\",\"WorkspaceId\":\"My Workspace\",\"ObjectId\":\"Partner Center Analytics\",\"DashboardId\":\"1f317786-158b-48a2-8e7e-3281526efda2\",\"Datasets\":[{\"DatasetId\":\"3166155f-6efe-4431-8755-92c88c7b4afc\",\"DatasetName\":\"Partner Center Analytics\"}]}," +
				"{\"Id\":\"f3a3e9a8-3293-4abb-df40-8ee46eb4df49\",\"RecordType\":20,\"CreationTime\":\"2017-09-25T21:17:24\",\"Operation\":\"CreateDataset\",\"OrganizationId\":\"4fd354cb-e69d-49f6-b954-daf93d5cef95\",\"UserType\":0,\"UserKey\":\"1003BFFDA4AB4F51\",\"Workload\":\"PowerBI\",\"UserId\":\"Nick@nealanalytics.com\",\"ClientIP\":\"67.183.153.248\",\"UserAgent\":\"Mozilla\\/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit\\/537.36 (KHTML\",\"Activity\":\"CreateDataset\",\"ItemName\":\"Partner Center Analytics\",\"WorkSpaceName\":\"My Workspace\",\"DatasetName\":\"Partner Center Analytics\",\"WorkspaceId\":\"My Workspace\",\"ObjectId\":\"Partner Center Analytics\",\"DatasetId\":\"3166155f-6efe-4431-8755-92c88c7b4afc\",\"DataConnectivityMode\":\"Import\"}," +
				"{\"Id\":\"2c5d9810-ab57-60e0-597a-d7eb3e8a0b08\",\"RecordType\":20,\"CreationTime\":\"2017-09-25T21:17:29\",\"Operation\":\"ViewDashboard\",\"OrganizationId\":\"4fd354cb-e69d-49f6-b954-daf93d5cef95\",\"UserType\":0,\"UserKey\":\"1003BFFDA4AB4F51\",\"Workload\":\"PowerBI\",\"UserId\":\"Nick@nealanalytics.com\",\"ClientIP\":\"67.183.153.248\",\"UserAgent\":\"Mozilla\\/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit\\/537.36 (KHTML\",\"Activity\":\"ViewDashboard\",\"ItemName\":\"Partner Center Analytics\",\"WorkSpaceName\":\"My Workspace\",\"DashboardName\":\"Partner Center Analytics\",\"WorkspaceId\":\"My Workspace\",\"ObjectId\":\"Partner Center Analytics\",\"DashboardId\":\"1f317786-158b-48a2-8e7e-3281526efda2\",\"Datasets\":[{\"DatasetId\":\"3166155f-6efe-4431-8755-92c88c7b4afc\",\"DatasetName\":\"Partner Center Analytics\"}]}," +
				"{\"Id\":\"02c1a902-58e6-04f3-2082-b40630ceb037\",\"RecordType\":20,\"CreationTime\":\"2017-09-25T21:17:32\",\"Operation\":\"ViewDashboard\",\"OrganizationId\":\"4fd354cb-e69d-49f6-b954-daf93d5cef95\",\"UserType\":0,\"UserKey\":\"1003BFFDA4AB4F51\",\"Workload\":\"PowerBI\",\"UserId\":\"Nick@nealanalytics.com\",\"ClientIP\":\"67.183.153.248\",\"UserAgent\":\"Mozilla\\/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit\\/537.36 (KHTML\",\"Activity\":\"ViewDashboard\",\"ItemName\":\"Partner Center Analytics\",\"WorkSpaceName\":\"My Workspace\",\"DashboardName\":\"Partner Center Analytics\",\"WorkspaceId\":\"My Workspace\",\"ObjectId\":\"Partner Center Analytics\",\"DashboardId\":\"1f317786-158b-48a2-8e7e-3281526efda2\",\"Datasets\":[{\"DatasetId\":\"3166155f-6efe-4431-8755-92c88c7b4afc\",\"DatasetName\":\"Partner Center Analytics\"}]}," +
				"{\"Id\":\"2b4dc608-6dac-ecf2-63de-531748688949\",\"RecordType\":20,\"CreationTime\":\"2017-09-25T21:17:51\",\"Operation\":\"ViewReport\",\"OrganizationId\":\"4fd354cb-e69d-49f6-b954-daf93d5cef95\",\"UserType\":0,\"UserKey\":\"1003BFFDA4AB4F51\",\"Workload\":\"PowerBI\",\"UserId\":\"Nick@nealanalytics.com\",\"ClientIP\":\"10.0.0.122\",\"UserAgent\":\"Mozilla\\/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit\\/537.36 (KHTML, like Gecko) Chrome\\/61.0.3163.91 Safari\\/537.36\",\"Activity\":\"ViewReport\",\"ItemName\":\"Partner Center Analytics\",\"WorkSpaceName\":\"My Workspace\",\"DatasetName\":\"Partner Center Analytics\",\"ReportName\":\"Partner Center Analytics\",\"WorkspaceId\":\"My Workspace\",\"ObjectId\":\"Partner Center Analytics\",\"DatasetId\":\"3166155f-6efe-4431-8755-92c88c7b4afc\",\"ReportId\":\"ddd6ecef-fb3c-4e99-b8bf-0ccf46214c15\"}," +
				"{\"Id\":\"02139176-943e-9d61-cd3e-7d9f93c54227\",\"RecordType\":20,\"CreationTime\":\"2017-09-25T21:19:01\",\"Operation\":\"CreateGroup\",\"OrganizationId\":\"4fd354cb-e69d-49f6-b954-daf93d5cef95\",\"UserType\":0,\"UserKey\":\"1003BFFDA4AB4F51\",\"Workload\":\"PowerBI\",\"UserId\":\"Nick@nealanalytics.com\",\"ClientIP\":\"67.183.153.248\",\"UserAgent\":\"Mozilla\\/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit\\/537.36 (KHTML\",\"Activity\":\"CreateGroup\",\"ItemName\":\"MPC Analytics\",\"MembershipInformation\":[{\"MemberEmail\":\"emmet@nealanalytics.com\"},{\"MemberEmail\":\"davidb@nealanalytics.com\"}],\"ObjectId\":\"MPC Analytics\"}]";
			var deserializeObject = JsonConvert.DeserializeObject<List<ExpandoObject>>(theString);
			dynamic firstRecord = deserializeObject.First();
			List<object> recordList = new List<object>();
			recordList.AddRange(deserializeObject);

			var members = GetTableInfo(recordList, firstRecord.Workload, string.Empty);
			Console.ReadLine();
		}

		public static List<DataTable> GetTableInfo(List<object> records, string tableName, string fkName)
		{
			var dtList = new List<DataTable>();
			var parentDt = new DataTable(tableName);
			dtList.Add(parentDt);
			IDictionary<string, Type> seenTypes = new Dictionary<string, Type>();
			if (!String.IsNullOrEmpty(fkName))
				parentDt.Columns.Add(fkName, typeof(string));
			foreach (object record in records)
			{
				ExpandoObject expando = (ExpandoObject)record;
				var newTypes = GetMemberInfo(expando);
				foreach (var newType in newTypes)
				{
					if (seenTypes.ContainsKey(newType.Key)) continue;
					if (newType.Value != typeof(List<object>))
					{
						seenTypes.Add(newType.Key, newType.Value);
						parentDt.Columns.Add(newType.Key, newType.Value);
					}
					else
					{
						var newExpandoList = ((IDictionary<string, object>)expando)[newType.Key];
						var subTables = GetTableInfo((List<object>)newExpandoList, $"{tableName}_{newType.Key}", tableName + "_id");
						dtList.AddRange(subTables);
						seenTypes.Add(newType.Key, typeof(List<Object>));
					}
				}
			}


			return dtList;
		}

		public static IDictionary<string, Type> GetMemberInfo(ExpandoObject target)
		{
			var tList = new List<string>();
			var memberInfos = new Dictionary<string, Type>();
			tList.AddRange(((IDynamicMetaObjectProvider)target).GetMetaObject(Expression.Constant(target)).GetDynamicMemberNames());
			foreach (var item in tList)
			{
				if (memberInfos.ContainsKey(item)) continue;
				memberInfos.Add(item, ((IDictionary<string, object>)target)[item].GetType());
			}

			return memberInfos;
		}
	}


}
