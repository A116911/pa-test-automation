--Validate table schema, table name, column name, mandatory, datatype, columnlength, columnprecision, columndistribution, distributionkey
WITH HLS (targetschema, TARGETTABLENAME, targetfieldname)
AS
(
SELECT DISTINCT tmp.targetschema,TARGETTABLENAME, tmp.targetfieldname FROM STAGING.Source_Target_Mapping_TEMP tmp
WHERE --tmp.TARGETTABLENAME = 'sat_ISU_PrintDocument' and
--CATEGORY = 'link' and
--tmp.targetfieldname = 'EDISENDDATE' and
tmp.targetfieldname not in ('LoadAuditId','LoadDateTime','LoadToolCode','RecordSource','DI_SEQUENCE_NUMBER','DS_UPDATE_TS')  and
tmp.ModelPowerDesigner = 'CHRN_BillingHistoryIM'
)
(SELECT isc.table_schema,
isc.table_name as actualtablename, 
isc.column_name as actualcolumnname, 
--isc.column_default,
case 
when isc.is_nullable = 'NO' then 'TRUE'
when isc.is_nullable = 'YES' then 'FALSE'
END as mandatory, 
isc.data_type as actualdatatype,
case 
when isc.data_type = 'int' then isc.numeric_precision
when isc.data_type = 'decimal' then isc.numeric_precision
else isnull(isc.character_maximum_length, 0)
END as actualcolumnlength,
case 
when isc.data_type = 'int' then 0
when isc.data_type = 'decimal' then isc.numeric_scale
else isnull(isc.numeric_scale, 0)
END as actualcolumnprecision,
ptds.distribution_policy_desc as actualcolumndistribution, 
case 
when pcds.distribution_ordinal = '1' then 'TRUE'
when pcds.distribution_ordinal = '0' then 'FALSE'
END as actualdistributionkey 
FROM INFORMATION_SCHEMA.COLUMNS isc
inner join sys.objects so on isc.table_name = so.name
inner join sys.schemas ss on so.schema_id = ss.schema_id and isc.table_schema = ss.name
inner join sys.pdw_table_distribution_properties ptds on so.object_id = ptds.object_id
inner join sys.pdw_column_distribution_properties pcds on so.object_id = pcds.object_id and isc.ordinal_position = pcds.column_id
INNER JOIN HLS ON isc.table_name = HLS.TARGETTABLENAME and isc.column_name = HLS.targetfieldname and isc.table_schema = HLS.targetschema
where isc.column_name not in ('HashDiff') 
--and isc.table_name = 'sat_ISU_PrintDocument'
--and isc.column_name in ('TOTAL_AMNT')
)
except
(
select distinct t.targetschema,T.targettablename,t.targetfieldname,
/* 
case
when t.DEFAULTVALUE = '0' then NULL
else nullif(t.DEFAULTVALUE,'')
END as expecteddefaultvalue,
*/
mandatory,
lower(t.targetdatatype) as expectedcolumntype,
isnull(nullif(lower(t.targetlength),'null'), 0) as expectedcolumnlength,
isnull(nullif(lower(t.targetprecisionlength),'null'), 0) as expectedprecisionlength,
t.distribution_policy,
t.distribution_key from [staging].Source_Target_Mapping_TEMP T
INNER JOIN HLS ON T.targettablename = HLS.TARGETTABLENAME and T.targetfieldname = HLS.targetfieldname and T.targetschema = HLS.targetschema
--where t.targetfieldname not in ('LoadAuditId','LoadDateTime','LoadToolCode','RecordSource','HashDiff','DI_SEQUENCE_NUMBER')
--wherevT.targettablename = 'sat_CRM_ActivityHeaderGuid'
--and T.targetfieldname in ('CAT_ID')
);
