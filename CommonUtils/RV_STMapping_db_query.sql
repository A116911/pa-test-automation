--Query to get hub tables for source table
select distinct hub.Staging_Table_Name, hub.Source_Field_Name,
concat(hub.Target_Table_Schema, '.', hub.Target_Table_Name) as Target_Table_Name, hub.Target_Column_Name
from [dbo].[DG_Mapping_Hubs] hub
where 
hub.Staging_Table_Name like '%stg_isu_fkkmako';
hub.Target_Table_Name in
('hub_Installation',
'hub_Premise')
and hub.Staging_Table_Name <> '';

dv_commercial.hub_Customer
dv_commercial.hub_ContractAccount
dv_commercial.hub_FICADocument
dv_commercial.hub_ReferenceSpecification


--Query to get link tables for source table
select * from [dbo].[DG_Mapping_Links] link
where 
--link.Target_Table_Name = 'link_CustomerContractAccount';
link.Staging_Table_Name like '%stg_isu_fkkmako';

--Query to get satellite tables for hub_link_reference
select sat.Staging_Table
--distinct sat.Target_Table_Name, sat.Staging_Table_Name, sat.Hub_Or_Link_Reference 
from [dbo].[DG_Mapping_Sat] sat
where Hub_Or_Link_Reference like 'link%';

sat.Target_Table_Name in 
('sat_ISU_Operand');

sat.Staging_Table_Name = 'stg_isu_eanlh' and
sat.Target_Column_Name in 
('InstallationId',
'ANLART',
'DEREGSTAT',
'HAZARD',
'DOGCODE',
'LOEVM',
'PremiseId');


--Query to get satellite table name along with references hub and link names
select distinct sat.Staging_Table_Name,concat(sat.Target_Table_Schema, '.', sat.Target_Table_Name) as Target_Table_Name, sat.Hub_Or_Link_Reference
from [dbo].[DG_Mapping_Sat] sat,
(select distinct link.Target_Table_Schema,link.Target_Table_Name from [dbo].[DG_Mapping_Links] link
--where link.Hub_Reference = 'hub_Customer'
union all
select distinct hub.Target_Table_Schema,hub.Target_Table_Name from [dbo].[DG_Mapping_Hubs] hub
--where hub.Target_Table_Name = 'hub_Customer'
) hublink_tar_schema_table
where 
sat.Hub_Or_Link_Reference = hublink_tar_schema_table.Target_Table_Name and
sat.Staging_Table_Name like '%stg_isu_fkkmako' and
sat.Target_Table_Name = 'sat_ISU_HardshipApplication';


--Query to get satellite field names (comma separated)
select 
--distinct sat.Staging_Table_Name,concat(sat.Target_Table_Schema, '.', sat.Target_Table_Name) as Target_Table_Name, sat.Hub_Or_Link_Reference
sat.Staging_Table_Name, sat.Source_Field_Name,concat(sat.Target_Table_Schema, '.', sat.Target_Table_Name) as Target_Table_Name, sat.Target_Column_Name, sat.Hub_Or_Link_Reference
--string_agg(sat.Source_Field_Name,',') as src_columns, string_agg(sat.Target_Column_Name,',') as tar_columns
from [dbo].[DG_Mapping_Sat] sat,
(select distinct link.Target_Table_Schema,link.Target_Table_Name from [dbo].[DG_Mapping_Links] link
--where link.Hub_Reference = 'hub_Customer'
union all
select distinct hub.Target_Table_Schema,hub.Target_Table_Name from [dbo].[DG_Mapping_Hubs] hub
--where hub.Target_Table_Name = 'hub_Customer'
) hublink_tar_schema_table
where 
sat.Hub_Or_Link_Reference = hublink_tar_schema_table.Target_Table_Name and
sat.Staging_Table_Name like '%stg_isu_zifitt014' and
sat.Target_Table_Name = 'sat_ISU_CustomerContractAccountDunningHistory'
and sat.Source_Field_Name = 'ZZCHILDRENS';




order by sat.Target_Column_Name;


select 
--distinct ref.Staging_Table_Name,ref.Target_Table_Schema,ref.Target_Table_Name 
string_agg(ref.Source_Field_Name,',') as src_columns, string_agg(ref.Target_Column_Name,',') as tar_columns 
from [dbo].[DG_Mapping_Ref] as ref
where ref.Staging_Table_Name like '%stg_isu_etrfv'
and ref.Target_Table_Name like '%ref_RateSteps%';

