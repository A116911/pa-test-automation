--Query to get hub tables for source table
select distinct hub.Staging_Table_Name, hub.Target_Table_Schema, 
hub.Target_Table_Name from [dbo].[DG_Mapping_Hubs] hub
where 
hub.Staging_Table_Name like '%stg_isu_adrc%';
hub.Target_Table_Name in
('hub_Installation',
'hub_Premise')
and hub.Staging_Table_Name <> '';


--Query to get link tables for source table
select * from [dbo].[DG_Mapping_Links] link
where 
--link.Target_Table_Name = 'link_ConnectionObjectLocationAddress';
link.Staging_Table_Name like '%stg_isu_adrc%';

--Query to get satellite tables for hub_link_reference
select distinct sat.Target_Table_Name, sat.Staging_Table_Name, sat.Hub_Or_Link_Reference from [dbo].[DG_Mapping_Sat] sat
where sat.Target_Table_Name in 
('sat_ISU_ConnectionObjectLocationAddress');

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
select distinct sat.Staging_Table_Name,sat.Target_Table_Schema,sat.Target_Table_Name, sat.Hub_Or_Link_Reference
from [dbo].[DG_Mapping_Sat] sat,
(select distinct link.Target_Table_Schema,link.Target_Table_Name from [dbo].[DG_Mapping_Links] link
--where link.Hub_Reference = 'hub_Customer'
union all
select distinct hub.Target_Table_Schema,hub.Target_Table_Name from [dbo].[DG_Mapping_Hubs] hub
--where hub.Target_Table_Name = 'hub_Customer'
) hublink_tar_schema_table
where 
sat.Hub_Or_Link_Reference = hublink_tar_schema_table.Target_Table_Name and
--sat.Staging_Table_Name like '%stg_isu_evbs' and
sat.Target_Table_Name = 'sat_ISU_ConnectionObjectPremise';


--Query to get satellite field names (comma separated)
select 
--distinct sat.Staging_Table_Name,sat.Target_Table_Schema,sat.Target_Table_Name, sat.Hub_Or_Link_Reference, 
sat.Source_Field_Name,sat.Target_Column_Name
--string_agg(sat.Target_Column_Name,',')
from [dbo].[DG_Mapping_Sat] sat,
(select distinct link.Target_Table_Schema,link.Target_Table_Name from [dbo].[DG_Mapping_Links] link
--where link.Hub_Reference = 'hub_Customer'
union all
select distinct hub.Target_Table_Schema,hub.Target_Table_Name from [dbo].[DG_Mapping_Hubs] hub
--where hub.Target_Table_Name = 'hub_Customer'
) hublink_tar_schema_table
where 
sat.Hub_Or_Link_Reference = hublink_tar_schema_table.Target_Table_Name and
--sat.Staging_Table_Name like '%stg_isu_evbs'
sat.Target_Table_Name = 'sat_ISU_ConnectionObjectPremise';
order by sat.Target_Column_Name;



