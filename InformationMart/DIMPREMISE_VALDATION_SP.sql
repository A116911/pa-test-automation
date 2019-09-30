/****** Object:  StoredProcedure [staging].[validate_full_load_dimpremise]    Script Date: 18/09/2019 10:13:45 AM ******/

IF OBJECT_ID('staging.validate_full_load_dimpremise', 'P') IS NOT NULL
	DROP PROCEDURE staging.validate_full_load_dimpremise
GO

CREATE PROCEDURE [staging].[validate_full_load_dimpremise] 
 @SRCAS [NVARCHAR](MAX)
,@ISCREATEEXPECTEDDIMTABLE [BIT]
,@ISEXECUTEVALIDATION [BIT]
,@ISDROPTEMPTABLE [BIT] 
AS

--Stored proc to test Full Load in dim_premise table
/*
Source tables:
ADRC - sat_ISU_Address
EVBS - dv_commercial.sat_ISU_ConnectionObjectPremise, dv_commercial.sat_ISU_Premise
EANL - sat_ISU_Installation
ILOA - sat_ISU_ConnectionObjectLocationAddress
*/
BEGIN
	BEGIN TRY
		DECLARE @SATELLITEQUERY NVARCHAR(MAX) -- Store final satellite select query
		DECLARE @DIMPREMISEQUERY NVARCHAR(MAX) -- Store final dim_installation select query
		DECLARE @DefaultCurrentDate DATETIME2 = '9999-12-31 00:00:00.000'

		--DECLARE @ALLSTAGINGTABLENAMES NVARCHAR(MAX)
		--DECLARE @STAGINGSCHEMANAME NVARCHAR(MAX)
		--DECLARE @STAGINGTABLENAME NVARCHAR(MAX)
		--SET @STAGINGSCHEMANAME = 'staging'
		--SET @STAGINGTABLENAME = 'stg_isu_eanl'
		SET @SATELLITEQUERY = '(
							select 
								CHECK_STATUS, DPID,
								PremiseId,
								ZZ_HP_PROPERTY_TYPE,ZZAMENITY_POOLHEATER,ZZAMENITY_POOLPUMP,ZZAMENITY_POOLSIZE,ZZAMENITY_SPA,
								ZZBAR_REFRIG,ZZBUS_ACTIVITY,ZZCHILER_FREEZER,ZZCOOKTOP,ZZCOOL_ROOMS, ZZDAYS_OPEN,
								ZZGEN_TYPE,ZZHEATING_PRES,ZZHOTWATER_FUEL,ZZHOTWATER_TYPE,ZZOPER_HOURS,ZZOVEN,
								ZZPRINTERS,ZZPROPERTY_USE,ZZREF_FREZ_TOP,ZZREFRI_UPRIGHT,ZZREFRIG_SBS,ZZSOLAR_WATER								
								,SiteUnitNumber,SiteStreetName,SiteStreetNumber,SiteSuburb,SiteState,SitePostalcode,FullAddress
								,RecordStartDate	
								,RecordEndDate
								,RecordDeletionIndicator								
							from staging.EXPECTED_DIMPREMISE_Auto														
							)'
		
		SET @DIMPREMISEQUERY = '(select 
									CheckStatus,DPID,Premise,PropertyType,PoolHeater,PoolPumpAge,PoolSize,SPAFlag,BarRefrigerators,BusinessActivity,
									ChillerFreezer,CookTopType,CoolsRooms,DaysOpenInWeek,GenerationType,HeatingSystemType,HotWaterFuel,HotWaterType,OpeningHours,
									OvenType,NumberOfPrinters,PropertyUse,FreezerOnTop,RefrigeratorUpright,RefrigerationSideBySideFlag,SolarHotWaterFlag
									,Unit,Street,StreetNumber,Suburb,State,Postcode,FullAddress									
									,RecordStartDate
									,RecordEndDate
									,RecordDeletionIndicator
									from [commercial].[dim_Premise] dp
																
								)'

		IF @ISCREATEEXPECTEDDIMTABLE = 1
		BEGIN

									
		--Create TMP table by joining satellite tables of EVBS, ADRC, ILOA table
		/*
		ADRC - sat_ISU_Address
		EVBS - sat_ISU_ConnectionObjectPremise, sat_ISU_Premise		
		ILOA - sat_ISU_ConnectionObjectLocationAddress
		*/
		IF OBJECT_ID('staging.TMPAUTO_COP_PREM_COLA_ADDRESS', 'U') IS NOT NULL
			DROP TABLE staging.TMPAUTO_COP_PREM_COLA_ADDRESS

		CREATE TABLE staging.TMPAUTO_COP_PREM_COLA_ADDRESS WITH 
		(
			DISTRIBUTION = HASH(PremiseId)
		)
		AS
		select 
		evbs_p.CHECK_STATUS, evbs_p.DPID,
		evbs_p.PremiseId,
		evbs_p.ZZ_HP_PROPERTY_TYPE,evbs_p.ZZAMENITY_POOLHEATER,evbs_p.ZZAMENITY_POOLPUMP,evbs_p.ZZAMENITY_POOLSIZE,evbs_p.ZZAMENITY_SPA,
		evbs_p.ZZBAR_REFRIG,evbs_p.ZZBUS_ACTIVITY,evbs_p.ZZCHILER_FREEZER,evbs_p.ZZCOOKTOP,evbs_p.ZZCOOL_ROOMS, evbs_p.ZZDAYS_OPEN,
		evbs_p.ZZGEN_TYPE,evbs_p.ZZHEATING_PRES,evbs_p.ZZHOTWATER_FUEL,evbs_p.ZZHOTWATER_TYPE,evbs_p.ZZOPER_HOURS,evbs_p.ZZOVEN,
		evbs_p.ZZPRINTERS,evbs_p.ZZPROPERTY_USE,evbs_p.ZZREF_FREZ_TOP,evbs_p.ZZREFRI_UPRIGHT,evbs_p.ZZREFRIG_SBS,evbs_p.ZZSOLAR_WATER			
		,adrc.roomnumber
		,adrc.floor
		,adrc.str_suppl1
		,adrc.str_suppl2
		,adrc.str_suppl3
		,adrc.house_num1
		,adrc.street
		,evbs_p.ds_update_ts  as RecordStartDate
		--,'RecordEndDate'  as RecordEndDate
		--,'RecordDeletionIndicator'  as RecordDeletionIndicator
		,'Premise_SK' as Premise_SK
		,'HashDiff'  as HashDiff
		,evbs_p.LoadDateTime
		,'CreatedBy'  as CreatedBy
		,'LoadAuditId'  as LoadAuditId
		,'LoadToolCode' as LoadToolCode
		--Extra Fields
		,evbs_cop.PremiseId as _PremiseId
		,evbs_cop.ConnectionObjectId as _ConnectionObjectId
		,iloa.AddressNumber	as 	_AddressNumber
		,evbs_p.di_operation_type
		,evbs_p.di_sequence_number	
		from 
		dv_commercial.sat_ISU_ConnectionObjectPremise evbs_cop
		left join dv_commercial.sat_ISU_Premise evbs_p
			on evbs_cop.PremiseId = evbs_p.PremiseId
		left join dv_commercial.sat_ISU_ConnectionObjectLocationAddress iloa
			on evbs_cop.ConnectionObjectId = iloa.ConnectionObjectId
		left join dv_customer.sat_ISU_Address adrc
			on iloa.AddressNumber = adrc.AddressNumber
		--WHERE evbs_cop.PremiseId = '2000126381'
/*		
		--Create TMP table for latest records in EANL - sat_ISU_Installation
		IF OBJECT_ID('staging.TMPAUTO_SAT_ISU_INSTALLATION', 'U') IS NOT NULL
			DROP TABLE staging.TMPAUTO_SAT_ISU_INSTALLATION

		CREATE TABLE staging.TMPAUTO_SAT_ISU_INSTALLATION WITH 
		(
			DISTRIBUTION = HASH(InstallationId)
		)
		AS
		SELECT 
		main_src.InstallationId,
		main_src.PremiseId
		FROM [dv_commercial].[sat_ISU_Installation] as main_src
		INNER JOIN
		(
		SELECT 
		PremiseId,
		InstallationId, 
		ds_update_ts,
		--max(DS_UPDATE_TS) as DS_UPDATE_TS,
		row_number() over(partition by PremiseId, ds_update_ts order by ds_update_ts desc, di_sequence_number desc) as rn
		--max(DI_SEQUENCE_NUMBER) as DI_SEQUENCE_NUMBER		
		FROM [dv_commercial].[sat_ISU_Installation]
		) as max_rec
			ON main_src.InstallationId = max_rec.InstallationId
			AND main_src.DS_UPDATE_TS = max_rec.ds_update_ts
			AND main_src.PremiseId = max_rec.PremiseId
			AND max_rec.rn = 1

		
		--Create TMP table for COP_PREM_COLA_ADDRESS AND INSTALLATION
		IF OBJECT_ID('staging.TMPAUTO_COP_PREM_COLA_ADDRESS_INSTAL', 'U') IS NOT NULL
			DROP TABLE staging.TMPAUTO_COP_PREM_COLA_ADDRESS_INSTAL

		CREATE TABLE staging.TMPAUTO_COP_PREM_COLA_ADDRESS_INSTAL WITH 
		(
			DISTRIBUTION = HASH(PremiseId)
		)
		AS
		SELECT 
		CHECK_STATUS, DPID,
		tmpcpca.PremiseId,
		ZZ_HP_PROPERTY_TYPE,ZZAMENITY_POOLHEATER,ZZAMENITY_POOLPUMP,ZZAMENITY_POOLSIZE,ZZAMENITY_SPA,
		ZZBAR_REFRIG,ZZBUS_ACTIVITY,ZZCHILER_FREEZER,ZZCOOKTOP,ZZCOOL_ROOMS, ZZDAYS_OPEN,
		ZZGEN_TYPE,ZZHEATING_PRES,ZZHOTWATER_FUEL,ZZHOTWATER_TYPE,ZZOPER_HOURS,ZZOVEN,
		ZZPRINTERS,ZZPROPERTY_USE,ZZREF_FREZ_TOP,ZZREFRI_UPRIGHT,ZZREFRIG_SBS,ZZSOLAR_WATER		
		,tmpins.InstallationId as InstallationId								
		,roomnumber
		,floor
		,str_suppl1
		,str_suppl2
		,str_suppl3
		,house_num1
		,street
		,RecordStartDate
		--,RecordEndDate
		--,RecordDeletionIndicator
		,Premise_SK
		,HashDiff
		,LoadDateTime
		,CreatedBy
		,LoadAuditId
		,LoadToolCode
		--Extra Fields
		,_PremiseId
		,_ConnectionObjectId
		,_AddressNumber
		,tmpcpca.di_operation_type
		FROM staging.TMPAUTO_COP_PREM_COLA_ADDRESS tmpcpca
		LEFT JOIN staging.TMPAUTO_SAT_ISU_INSTALLATION tmpins
			ON tmpcpca.PremiseId = tmpins.PremiseId		
*/
		
		--Create TMP table for COP_PREM_COLA_ADDRESS_INSTALLATION AND ComputedSatellite
		IF OBJECT_ID('staging.TMPAUTO_COP_PREM_COLA_ADDRESS_INSTAL_COMPSAT', 'U') IS NOT NULL
			DROP TABLE staging.TMPAUTO_COP_PREM_COLA_ADDRESS_INSTAL_COMPSAT

		CREATE TABLE staging.TMPAUTO_COP_PREM_COLA_ADDRESS_INSTAL_COMPSAT WITH 
		(
			DISTRIBUTION = HASH(PremiseId)
		)
		AS
		SELECT 
		tmpcpca.CHECK_STATUS, tmpcpca.DPID,
		tmpcpca.PremiseId,
		ZZ_HP_PROPERTY_TYPE,ZZAMENITY_POOLHEATER,ZZAMENITY_POOLPUMP,ZZAMENITY_POOLSIZE,ZZAMENITY_SPA,
		ZZBAR_REFRIG,ZZBUS_ACTIVITY,ZZCHILER_FREEZER,ZZCOOKTOP,ZZCOOL_ROOMS, ZZDAYS_OPEN,
		ZZGEN_TYPE,ZZHEATING_PRES,ZZHOTWATER_FUEL,ZZHOTWATER_TYPE,ZZOPER_HOURS,ZZOVEN,
		ZZPRINTERS,ZZPROPERTY_USE,ZZREF_FREZ_TOP,ZZREFRI_UPRIGHT,ZZREFRIG_SBS,ZZSOLAR_WATER		
		--,cpcai.InstallationId as InstallationId								
		,satc_prem.SiteUnitNumber
		,satc_prem.SiteStreetNumber
		,satc_prem.SiteStreetName
		,satc_prem.SiteSuburb
		,satc_prem.SiteState
		,satc_prem.SitePostalcode
		,CONCAT
		(
			LTRIM(RTRIM(ISNULL(satc_prem.SiteUnitNumber, ''))), ' ',
			LTRIM(RTRIM(ISNULL(satc_prem.SiteStreetNumber, ''))), ' ',
			LTRIM(RTRIM(ISNULL(satc_prem.SiteStreetName, ''))), ' ',
			LTRIM(RTRIM(ISNULL(satc_prem.SiteSuburb, ''))), ' ',
			LTRIM(RTRIM(ISNULL(satc_prem.SiteState, ''))), ' ',
			LTRIM(RTRIM(ISNULL(satc_prem.SitePostalcode, '')))
		)
		AS FullAddress
		,tmpcpca.RecordStartDate
		--,tmpcpca.RecordEndDate
		--,tmpcpca.RecordDeletionIndicator
		,tmpcpca.Premise_SK
		--,cpcai.HashDiff
		,tmpcpca.LoadDateTime
		,tmpcpca.CreatedBy
		,tmpcpca.LoadAuditId
		,tmpcpca.LoadToolCode
		--Extra Fields
		,_PremiseId
		,_ConnectionObjectId
		,_AddressNumber
		,tmpcpca.roomnumber
		,tmpcpca.floor
		,tmpcpca.str_suppl1
		,tmpcpca.str_suppl2
		,tmpcpca.str_suppl3
		,tmpcpca.house_num1
		,tmpcpca.street
		,tmpcpca.di_operation_type
		,tmpcpca.di_sequence_number
		,satc_prem.ChangePointInTime as RecordStartDate_SatComp
		,ROW_NUMBER() OVER (PARTITION BY tmpcpca.PremiseId
					                                  ,tmpcpca.RecordStartDate
										  ORDER BY     tmpcpca.RecordStartDate DESC
													  ,satc_prem.ChangePointInTime DESC
										              ,tmpcpca.di_sequence_number DESC
													  ,tmpcpca.LOADDATETIME DESC
										  ) AS RN
		FROM
		staging.TMPAUTO_COP_PREM_COLA_ADDRESS tmpcpca
		LEFT JOIN [dv_commercial].[satcomp_PremiseAddressPOC] satc_prem
		ON tmpcpca.PremiseId = satc_prem.PremiseId
		and tmpcpca.RecordStartDate >= satc_prem.ChangePointInTime
		--and tmpcpca.PremiseId = '2000126381'
		--order by tmpcpca.RecordStartDate,tmpcpca.di_sequence_number,satc_prem.ChangePointInTime
		

		--Calculate HashDiff for TMPAUTO_COP_PREM_COLA_ADDRESS_INSTAL
		IF OBJECT_ID('staging.TMPAUTO_COP_PREM_COLA_ADDRESS_INSTAL_COMPSAT_HDiff', 'U') IS NOT NULL
			DROP TABLE staging.TMPAUTO_COP_PREM_COLA_ADDRESS_INSTAL_COMPSAT_HDiff

		CREATE TABLE staging.TMPAUTO_COP_PREM_COLA_ADDRESS_INSTAL_COMPSAT_HDiff WITH 
		(
			DISTRIBUTION = HASH(PremiseId)
		)
		AS
		SELECT 
		cpcaic.CHECK_STATUS, cpcaic.DPID,cpcaic.PremiseId,cpcaic.ZZ_HP_PROPERTY_TYPE,cpcaic.ZZAMENITY_POOLHEATER,cpcaic.ZZAMENITY_POOLPUMP,
		cpcaic.ZZAMENITY_POOLSIZE,cpcaic.ZZAMENITY_SPA,cpcaic.ZZBAR_REFRIG,cpcaic.ZZBUS_ACTIVITY,cpcaic.ZZCHILER_FREEZER,cpcaic.ZZCOOKTOP,cpcaic.ZZCOOL_ROOMS, 
		cpcaic.ZZDAYS_OPEN,cpcaic.ZZGEN_TYPE,cpcaic.ZZHEATING_PRES,cpcaic.ZZHOTWATER_FUEL,cpcaic.ZZHOTWATER_TYPE,cpcaic.ZZOPER_HOURS,cpcaic.ZZOVEN,cpcaic.ZZPRINTERS,
		cpcaic.ZZPROPERTY_USE,cpcaic.ZZREF_FREZ_TOP,cpcaic.ZZREFRI_UPRIGHT,cpcaic.ZZREFRIG_SBS,cpcaic.ZZSOLAR_WATER,
		cpcaic.SiteUnitNumber,cpcaic.SiteStreetNumber ,cpcaic.SiteStreetName ,cpcaic.SiteSuburb	,cpcaic.SiteState,cpcaic.SitePostalcode,cpcaic.FullAddress
		,cpcaic.RecordStartDate
		,cpcaic.Premise_SK		
		,cpcaic.LoadDateTime,cpcaic.CreatedBy,cpcaic.LoadAuditId,cpcaic.LoadToolCode
		--Extra Fields
		,cpcaic._PremiseId,cpcaic._ConnectionObjectId,cpcaic._AddressNumber,cpcaic.roomnumber,cpcaic.floor,cpcaic.str_suppl1,cpcaic.str_suppl2,cpcaic.str_suppl3
		,cpcaic.house_num1,cpcaic.street,cpcaic.di_operation_type,cpcaic.di_sequence_number,cpcaic.RecordStartDate_SatComp
		,HashDiff = CONVERT(BINARY(32), HASHBYTES ('SHA2_256', 
														CONCAT
											               (
																--LTRIM(RTRIM(ISNULL(InstallationId, ''))), '|',
																LTRIM(RTRIM(ISNULL(SiteUnitNumber, ''))), '|',
																LTRIM(RTRIM(ISNULL(SiteStreetNumber, ''))), '|',
																LTRIM(RTRIM(ISNULL(SiteStreetName, ''))), '|',
																LTRIM(RTRIM(ISNULL(SiteSuburb, ''))), '|',
																LTRIM(RTRIM(ISNULL(SiteState, ''))), '|',
																LTRIM(RTRIM(ISNULL(SitePostalcode, ''))), '|',																
																LTRIM(RTRIM(ISNULL(ZZPROPERTY_USE, ''))), '|',
																LTRIM(RTRIM(ISNULL(ZZHEATING_PRES, ''))), '|',
																LTRIM(RTRIM(ISNULL(ZZHOTWATER_FUEL, ''))), '|',
																LTRIM(RTRIM(ISNULL(ZZHOTWATER_TYPE, ''))), '|',
																LTRIM(RTRIM(ISNULL(ZZGEN_TYPE, ''))), '|',
																LTRIM(RTRIM(ISNULL(ZZSOLAR_WATER, ''))), '|',
																LTRIM(RTRIM(ISNULL(ZZPRINTERS, ''))), '|',
																LTRIM(RTRIM(ISNULL(ZZBUS_ACTIVITY, ''))), '|',
																LTRIM(RTRIM(ISNULL(ZZDAYS_OPEN, ''))), '|',
																LTRIM(RTRIM(ISNULL(ZZOPER_HOURS, ''))), '|',
																LTRIM(RTRIM(ISNULL(ZZREFRIG_SBS, ''))), '|',
																LTRIM(RTRIM(ISNULL(ZZREF_FREZ_TOP, ''))), '|',
																LTRIM(RTRIM(ISNULL(ZZCOOL_ROOMS, ''))), '|',
																LTRIM(RTRIM(ISNULL(ZZREFRI_UPRIGHT, ''))), '|',
																LTRIM(RTRIM(ISNULL(ZZCHILER_FREEZER, ''))), '|',
																LTRIM(RTRIM(ISNULL(ZZBAR_REFRIG, ''))), '|',
																LTRIM(RTRIM(ISNULL(DPID, ''))), '|',
																LTRIM(RTRIM(ISNULL(CHECK_STATUS, ''))), '|',
																LTRIM(RTRIM(ISNULL(ZZ_HP_PROPERTY_TYPE, ''))), '|',
																LTRIM(RTRIM(ISNULL(ZZAMENITY_POOLPUMP, ''))), '|',
																LTRIM(RTRIM(ISNULL(ZZAMENITY_POOLSIZE, ''))), '|',
																LTRIM(RTRIM(ISNULL(ZZAMENITY_POOLHEATER, ''))), '|',
																LTRIM(RTRIM(ISNULL(ZZAMENITY_SPA, ''))), '|',
																LTRIM(RTRIM(ISNULL(ZZOVEN, ''))), '|',
																LTRIM(RTRIM(ISNULL(ZZCOOKTOP, '')))
													        )
													)
							)
							,ROW_NUMBER() OVER (
								PARTITION BY cpcaic.PremiseId
											,cpcaic.RecordStartDate											 
								ORDER BY cpcaic.DI_SEQUENCE_NUMBER DESC
										,cpcaic.LOADDATETIME DESC
										,cpcaic.RecordStartDate_SatComp desc
						) AS RN		
		FROM staging.TMPAUTO_COP_PREM_COLA_ADDRESS_INSTAL_COMPSAT cpcaic
		WHERE cpcaic.RN = '1'
		--WHERE cpcaic.PremiseId = '3001346582'
		--ORDER BY cpcaic.RecordStartDate, cpcaic.DI_SEQUENCE_NUMBER
		

		--Calculate Previous HashDiff for TMPAUTO_COP_PREM_COLA_ADDRESS_INSTAL_COMPSAT_HDiff
		IF OBJECT_ID('staging.TMPAUTO_COP_PREM_COLA_ADDRESS_INSTAL_COMPSAT_HDiff_PrevHDiff', 'U') IS NOT NULL
			DROP TABLE staging.TMPAUTO_COP_PREM_COLA_ADDRESS_INSTAL_COMPSAT_HDiff_PrevHDiff

		CREATE TABLE staging.TMPAUTO_COP_PREM_COLA_ADDRESS_INSTAL_COMPSAT_HDiff_PrevHDiff WITH 
		(
			DISTRIBUTION = HASH(PremiseId)
		)
		AS
		SELECT
		a.*
		,LAG(HashDiff,1) OVER (PARTITION BY  a.PremiseId
							   ORDER BY a.RecordStartDate,a.RecordStartDate_SatComp
							   ) AS Previous_HashDiff
		FROM staging.TMPAUTO_COP_PREM_COLA_ADDRESS_INSTAL_COMPSAT_HDiff a
		WHERE a.RN = '1'
		--and a.PremiseId = '3001346582'
		--ORDER BY a.RecordStartDate, a.DI_SEQUENCE_NUMBER



		--Remove Duplicates and Add End Date
		IF OBJECT_ID('staging.RemoveDupAddEndDate_Premise_Auto', 'U') IS NOT NULL
			DROP TABLE staging.RemoveDupAddEndDate_Premise_Auto

		CREATE TABLE staging.RemoveDupAddEndDate_Premise_Auto WITH 
		(
			DISTRIBUTION = HASH(PremiseId)
		)
		AS
		SELECT
		a.*
		,LEAD(a.RecordStartDate,1, @DefaultCurrentDate) OVER (PARTITION BY  a.PremiseId
															ORDER BY a.RecordStartDate
															) AS RecordEndDate 
		FROM 
		(
		SELECT * FROM  staging.TMPAUTO_COP_PREM_COLA_ADDRESS_INSTAL_COMPSAT_HDiff_PrevHDiff WHERE Previous_HashDiff IS NULL
		UNION ALL 
		SELECT * FROM staging.TMPAUTO_COP_PREM_COLA_ADDRESS_INSTAL_COMPSAT_HDiff_PrevHDiff WHERE Previous_HashDiff <> HashDiff
		) as a

		--Update RecordEndDate and RecordDeletionFlag
		IF OBJECT_ID('staging.EXPECTED_DIMPREMISE_Auto', 'U') IS NOT NULL
			DROP TABLE staging.EXPECTED_DIMPREMISE_Auto

		CREATE TABLE staging.EXPECTED_DIMPREMISE_Auto WITH 
		(
			DISTRIBUTION = HASH(PremiseId)
		)
		AS
		SELECT
		CHECK_STATUS, DPID,
		Base.PremiseId,
		ZZ_HP_PROPERTY_TYPE,ZZAMENITY_POOLHEATER,ZZAMENITY_POOLPUMP,ZZAMENITY_POOLSIZE,ZZAMENITY_SPA,
		ZZBAR_REFRIG,ZZBUS_ACTIVITY,ZZCHILER_FREEZER,ZZCOOKTOP,ZZCOOL_ROOMS, ZZDAYS_OPEN,
		ZZGEN_TYPE,ZZHEATING_PRES,ZZHOTWATER_FUEL,ZZHOTWATER_TYPE,ZZOPER_HOURS,ZZOVEN,
		ZZPRINTERS,ZZPROPERTY_USE,ZZREF_FREZ_TOP,ZZREFRI_UPRIGHT,ZZREFRIG_SBS,ZZSOLAR_WATER								
		--,InstallationId
		,SiteUnitNumber,SiteStreetName,SiteStreetNumber,SiteSuburb,SiteState,SitePostalcode,FullAddress
		,RecordStartDate																
		,CASE WHEN Base.RecordEndDate <> @DefaultCurrentDate THEN DATEADD(ns,-100, Base.RecordEndDate) 
		      WHEN PhysicalDel.PremiseId IS NOT NULL THEN PhysicalDel.LatestDeletionDate
		      ELSE Base.RecordEndDate 
		 END AS RecordEndDate
		 ,CASE WHEN PhysicalDel.PremiseId IS NOT NULL THEN 'X' ELSE ''
		 END AS RecordDeletionIndicator
		 FROM 
		 staging.RemoveDupAddEndDate_Premise_Auto Base
		 LEFT JOIN 
	     (
		   SELECT
                   PremiseId
				  ,MAX(RecordStartDate) AS LatestDeletionDate
		   FROM 
		          staging.RemoveDupAddEndDate_Premise_Auto
		   WHERE
		         di_operation_type ='D'
		   GROUP BY
                  PremiseId
		  ) AS PhysicalDel
		ON 
          PhysicalDel.PremiseId = BASE.PremiseId
		AND BASE.RecordStartDate <= PhysicalDel.LatestDeletionDate
		
		END
		
		IF @ISEXECUTEVALIDATION = 1
		BEGIN
			PRINT @SRCAS + ' query: '
			PRINT @SATELLITEQUERY
			PRINT 'DIM_PREMISE query: '
			PRINT @DIMPREMISEQUERY
			
			PRINT 'Executing except query between Satellite and DIM_PREMISE'
			EXECUTE (@SATELLITEQUERY + ' EXCEPT ' + @DIMPREMISEQUERY)
			PRINT 'Executing except query between DIM_PREMISE and Satellite'
			EXECUTE (@DIMPREMISEQUERY + ' EXCEPT ' + @SATELLITEQUERY)
		END

		

	END TRY

	BEGIN CATCH
		SELECT ERROR_NUMBER() AS ErrORNumber
			,ERROR_SEVERITY() AS ErrORSeverity
			,ERROR_STATE() AS ErrORState
			,ERROR_PROCEDURE() AS ErrORProcedure
			,ERROR_MESSAGE() AS ErrORMessage;
	END CATCH
END
