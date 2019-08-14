--Create Source_Target_Mapping_TEMP table:
CREATE TABLE [staging].[Source_Target_Mapping_TEMP]
(
	[seqNo] [int] NULL,
	[category] [varchar](20) NULL,
	[sourceschemaname] [varchar](100) NULL,
	[sourcetablename] [varchar](100) NULL,
	[sourcefieldname] [varchar](100) NULL,
	[targetschema] [varchar](100) NULL,
	[targettablename] [varchar](100) NULL,
	[targetfieldname] [varchar](100) NULL,
	[reftableschema] [varchar](100) NULL,
	[reftable] [varchar](100) NULL,
	[reftabfield] [varchar](100) NULL,
	[reftabsequence] [int] NULL,
	[defaultvalue] [varchar](100) NULL,
	[mandatory] [nvarchar](500) NULL,
	[targetdatatype] [nvarchar](500) NULL,
	[targetlength] [nvarchar](500) NULL,
	[targetprecisionlength] [nvarchar](500) NULL,
	[distribution_policy] [nvarchar](500) NULL,
	[distribution_key] [nvarchar](500) NULL,
	[isprimarykey] [nvarchar](500) NULL,
	[ModelPowerDesigner] [varchar](255) NULL,
	[hub_link_reference] [varchar](255) NULL
)
WITH
(
	DISTRIBUTION = ROUND_ROBIN,
	CLUSTERED COLUMNSTORE INDEX
)


--Query to create temporary table with reftableschema (exp_schema), reftablefield(exp_fieldname)
CREATE TABLE staging.Source_Target_Mapping_TEMP2
WITH
(
    CLUSTERED COLUMNSTORE INDEX,
    DISTRIBUTION = HASH(targettablename)
)
AS (
select a.seqNo
,a.targettablename,a.targetfieldname,a.reftableschema,a.reftable,a.reftabfield,
a.reftabsequence,
b.targetschema as exp_schema,b.targettablename as exp_tablename,b.targetfieldname as exp_fieldname
--row_number() over (partition by a.sourcetablename, b.targettablename, a.reftable order by b.targetfieldname asc) as exp_tabsequence
FROM staging.Source_Target_Mapping_TEMP a
left join
(
select 
distinct targetschema,targettablename,targetfieldname,
isprimarykey,
ModelPowerDesigner
from staging.Source_Target_Mapping_TEMP
where ModelPowerDesigner = 'CHRN_BillingHistoryIM'
 --and reftable is not null 
--order by targettablename,targetfieldname asc
) b
ON b.targettablename = a.reftable
and b.targetfieldname = a.targetfieldname
and a.ModelPowerDesigner = b.ModelPowerDesigner
and a.isprimarykey = b.isprimarykey
where a.reftable is not null
and a.isprimarykey = 'TRUE'
and a.ModelPowerDesigner = 'CHRN_BillingHistoryIM'
--order by a.reftable,a.reftabfield
);


----Query to update reftableschema, reftablefield with exp_schema, exp_fieldname
UPDATE staging.Source_Target_Mapping_TEMP
SET 
Source_Target_Mapping_TEMP.reftableschema = Source_Target_Mapping_TEMP2.exp_schema,
Source_Target_Mapping_TEMP.reftabfield = Source_Target_Mapping_TEMP2.exp_fieldname
FROM staging.Source_Target_Mapping_TEMP2
where Source_Target_Mapping_TEMP.reftable = Source_Target_Mapping_TEMP2.reftable
and Source_Target_Mapping_TEMP.targettablename = Source_Target_Mapping_TEMP2.targettablename
and Source_Target_Mapping_TEMP.targetfieldname = Source_Target_Mapping_TEMP2.targetfieldname
and Source_Target_Mapping_TEMP.seqNo = Source_Target_Mapping_TEMP2.seqNo
and Source_Target_Mapping_TEMP.reftable is not null
and Source_Target_Mapping_TEMP.isprimarykey = 'TRUE'
and Source_Target_Mapping_TEMP.ModelPowerDesigner = 'CHRN_BillingHistoryIM';


