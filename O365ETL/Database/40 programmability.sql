SET ANSI_NULLS              ON;
SET ANSI_PADDING            ON;
SET ANSI_WARNINGS           ON;
SET ANSI_NULL_DFLT_ON       ON;
SET CONCAT_NULL_YIELDS_NULL ON;
SET QUOTED_IDENTIFIER       ON;
go


CREATE PROCEDURE [adtlog].[uspMoveStagingToPowerBI]
	
AS
BEGIN

BEGIN TRAN
DELETE  t
FROM [adtlog].[PowerBI] as t
INNER JOIN [adtlog].[staging_PowerBI] s 
ON t.Id = s.Id;

INSERT INTO [adtlog].[PowerBI]
           ([Id]
           ,[RecordType]
           ,[CreationTime]
           ,[Operation]
           ,[OrganizationId]
           ,[UserType]
           ,[UserKey]
           ,[Workload]
           ,[UserId]
           ,[ClientIP]
           ,[UserAgent]
           ,[Activity]
           ,[ItemName]
           ,[WorkSpaceName]
           ,[DashboardName]
           ,[WorkspaceId]
           ,[ObjectId]
           ,[DashboardId]
           ,[DatasetName]
           ,[ReportName]
           ,[DatasetId]
           ,[ReportId]
           ,[OrgAppPermission]
		   ,[BatchId])
SELECT DISTINCT 
			[Id]
           ,[RecordType]
           ,[CreationTime]
           ,[Operation]
           ,[OrganizationId]
           ,[UserType]
           ,[UserKey]
           ,[Workload]
           ,[UserId]
           ,[ClientIP]
           ,[UserAgent]
           ,[Activity]
           ,[ItemName]
           ,[WorkSpaceName]
           ,[DashboardName]
           ,[WorkspaceId]
           ,[ObjectId]
           ,[DashboardId]
           ,[DatasetName]
           ,[ReportName]
           ,[DatasetId]
           ,[ReportId]
           ,[OrgAppPermission]
		   ,[BatchId]
		   
FROM [adtlog].[staging_PowerBI]

TRUNCATE TABLE [adtlog].[staging_PowerBI]

DELETE t 
FROM [adtlog].[PowerBI_Datasets] as t
INNER JOIN [adtlog].[staging_PowerBI_Datasets] s 
ON t.PowerBI_id = s.PowerBI_id

INSERT INTO [adtlog].[PowerBI_Datasets]
           ([PowerBI_id]
           ,[DatasetId]
           ,[DatasetName])
SELECT DISTINCT 
			[PowerBI_id]
           ,[DatasetId]
           ,[DatasetName]
FROM [adtlog].[staging_PowerBI_Datasets]

TRUNCATE TABLE [adtlog].[staging_PowerBI]

DELETE t 
FROM [adtlog].[PowerBI_MembershipInfo] as t
INNER JOIN [adtlog].[staging_PowerBI_MembershipInfo] s 
ON t.PowerBI_id = s.PowerBI_id

INSERT INTO [adtlog].[PowerBI_MembershipInfo]
           ([PowerBI_id]
           ,[MemberEmail])
SELECT DISTINCT 
			[PowerBI_id]
           ,[MemberEmail]
FROM [adtlog].[staging_PowerBI_MembershipInfo]

TRUNCATE TABLE [adtlog].[staging_PowerBI_MembershipInfo]

COMMIT

END
go