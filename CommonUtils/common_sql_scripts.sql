SELECT isc.table_schema,isc.table_name,isc.column_name, isc.ordinal_position,isc.column_default,isc.is_nullable as mandatory, isc.data_type,isc.character_maximum_length, 
ptds.distribution_policy_desc, pcds.distribution_ordinal
FROM INFORMATION_SCHEMA.COLUMNS isc
inner join sys.objects so on isc.table_name = so.name
inner join sys.pdw_table_distribution_properties ptds on so.object_id = ptds.object_id
inner join sys.pdw_column_distribution_properties pcds on so.object_id = pcds.object_id and isc.ordinal_position = pcds.column_id
WHERE isc.table_name like '%hub_CRM_TransactionHeaderGuid%';


--Query to get staging table names with schema name
select 
--concat(ssc.name, '.', st.name)
--concat('select distinct ds_update_ts from ', ssc.name, '.', st.name, ';')
--concat('drop table ', ssc.name, '.', st.name, ';')
string_agg(concat('select * from ', ssc.name, '.', st.name, ' union all '),'') as schema_table
from sys.tables as st 
	inner join sys.objects so
	on st.object_id = so.object_id
	inner join sys.schemas ssc
	on ssc.schema_id = st.schema_id
where
ssc.name = 'staging' and --@STAGINGSCHEMANAME and
st.name like ('%stg_isu_fkkmako%');


