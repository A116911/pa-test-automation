/****** Object:  StoredProcedure [staging].[validate_full_load_diminstallationregisterrate]    Script Date: 7/10/2019 12:01:54 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
ALTER PROC [staging].[validate_full_load_diminstallationregisterrate] @SRCAS [NVARCHAR](MAX),@ISCREATEEXPECTEDDIMTABLE [BIT],@ISEXECUTEVALIDATION [BIT],@ISDROPTEMPTABLE [BIT] AS

--Stored proc to test Full Load in dim_InstallationRegisterRate table
/*
Source tables:
EASTS - sat_ISU_InstallationRegisterTimeSlice
*/
BEGIN
	BEGIN TRY
		DECLARE @SATELLITEQUERY NVARCHAR(MAX) -- Store final satellite select query
		DECLARE @DIMINSTALLATIONREGISTERRATEQUERY NVARCHAR(MAX) -- Store final dim_installation select query
		DECLARE @DefaultCurrentDate DATETIME2 = '9999-12-31 00:00:00.000'

		--DECLARE @ALLSTAGINGTABLENAMES NVARCHAR(MAX)
		--DECLARE @STAGINGSCHEMANAME NVARCHAR(MAX)
		--DECLARE @STAGINGTABLENAME NVARCHAR(MAX)
		--SET @STAGINGSCHEMANAME = 'staging'
		--SET @STAGINGTABLENAME = 'stg_isu_easts'
		SET @SATELLITEQUERY = '(
							select 
								Installation, LogicalRegisterNumber,RegisterValidFrom,RegisterValidTo,RegisterRateType,RegisterRateFactGroup,RegisterPriceClass
								,RecordStartDate
								,RecordEndDate
								,RecordDeletionIndicator
							from staging.EXPECTED_DIMINSTALLATIONREGISTERRATE_Auto								 
							)'
		
		SET @DIMINSTALLATIONREGISTERRATEQUERY = '(select 									
									Installation, LogicalRegisterNumber,RegisterValidFrom,RegisterValidTo,RegisterRateType,RegisterRateFactGroup,RegisterPriceClass
									,RecordStartDate
									,RecordEndDate
									,RecordDeletionflag
									from [commercial].[dim_InstallationRegisterRate]																
								)'

		IF @ISCREATEEXPECTEDDIMTABLE = 1
		BEGIN
		--Calculate hashDiff
		IF OBJECT_ID('staging.TMPAUTO_SAT_ISU_INSTALLATIONREGISTERTIMESLICE_HDIFF', 'U') IS NOT NULL
			DROP TABLE staging.TMPAUTO_SAT_ISU_INSTALLATIONREGISTERTIMESLICE_HDIFF
		CREATE TABLE staging.TMPAUTO_SAT_ISU_INSTALLATIONREGISTERTIMESLICE_HDIFF WITH 
		(
		DISTRIBUTION = HASH(InstallationId)
		)
		AS
		SELECT *,
		HashDiff = CONVERT(BINARY(32), HASHBYTES ('SHA2_256', 
														CONCAT
											               (
																--LTRIM(RTRIM(ISNULL(base.LogicalRegisterNumber, ''))), '|',																
																LTRIM(RTRIM(ISNULL(base.AB, ''))), '|',
																--LTRIM(RTRIM(ISNULL(base.TimeSliceExpirationDate, ''))), '|',
																LTRIM(RTRIM(ISNULL(base.RateType, ''))), '|',
																LTRIM(RTRIM(ISNULL(base.RateFactGroup, ''))), '|',
																LTRIM(RTRIM(ISNULL(base.PREISKLA, ''))), '|',
																LTRIM(RTRIM(ISNULL(CASE WHEN DI_OPERATION_TYPE='D' THEN 'D' ELSE NULL END, '')))
													        )
													)
							)		
		FROM 
		(
		SELECT
			InstallationId,
			LogicalRegisterNumber,
			AB,
			TimeSliceExpirationDate,
			RateType,
			RateFactGroup,
			PREISKLA,
			ds_update_ts as RecordStartDate,
			--Extra Fields
			LoadDateTime as LOADDATETIME,
			di_sequence_number as DI_SEQUENCE_NUMBER,
			di_operation_type as DI_OPERATION_TYPE,
			ROW_NUMBER() OVER (PARTITION BY InstallationId, LogicalRegisterNumber, TimeSliceExpirationDate, ds_update_ts
							   ORDER BY	di_sequence_number DESC
										,LoadDateTime DESC
							   ) as RN		
		FROM 
			dv_commercial.sat_ISU_InstallationRegisterTimeSlice
		) as base
		WHERE base.RN = 1
		
		
		--Calculate previous hashDiff
		IF OBJECT_ID('staging.TMPAUTO_SAT_ISU_INSTALLATIONREGISTERTIMESLICE_HDIFF_PREVHDIFF', 'U') IS NOT NULL
			DROP TABLE staging.TMPAUTO_SAT_ISU_INSTALLATIONREGISTERTIMESLICE_HDIFF_PREVHDIFF
		CREATE TABLE staging.TMPAUTO_SAT_ISU_INSTALLATIONREGISTERTIMESLICE_HDIFF_PREVHDIFF WITH 
		(
		DISTRIBUTION = HASH(InstallationId)
		)
		AS
		SELECT InstallationId,
			LogicalRegisterNumber,
			AB,
			TimeSliceExpirationDate,
			RateType,
			RateFactGroup,
			PREISKLA,
			RecordStartDate,
			--Extra Fields
			LOADDATETIME,
			DI_SEQUENCE_NUMBER,
			DI_OPERATION_TYPE,
			HashDiff,
			LAG(HashDiff,1) OVER (PARTITION BY  InstallationId, LogicalRegisterNumber, TimeSliceExpirationDate
							   ORDER BY RecordStartDate
							   ) AS Previous_HashDiff		
		FROM 
		staging.TMPAUTO_SAT_ISU_INSTALLATIONREGISTERTIMESLICE_HDIFF
		
		
		--Remove duplicate entries and add end date
		IF OBJECT_ID('staging.RemoveDupAddEndDate_INSTALLATIONREGISTERRATE_Auto', 'U') IS NOT NULL
			DROP TABLE staging.RemoveDupAddEndDate_INSTALLATIONREGISTERRATE_Auto

		CREATE TABLE staging.RemoveDupAddEndDate_INSTALLATIONREGISTERRATE_Auto WITH 
		(
			DISTRIBUTION = HASH(InstallationId)
		)
		AS
		SELECT
		a.*
		,LEAD(a.RecordStartDate,1, @DefaultCurrentDate) OVER (PARTITION BY  a.InstallationId, a.LogicalRegisterNumber, a.TimeSliceExpirationDate
															ORDER BY a.RecordStartDate
															) AS RecordEndDate
		FROM 
		(
			SELECT * FROM  staging.TMPAUTO_SAT_ISU_INSTALLATIONREGISTERTIMESLICE_HDIFF_PREVHDIFF WHERE Previous_HashDiff IS NULL
			UNION ALL 
			SELECT * FROM staging.TMPAUTO_SAT_ISU_INSTALLATIONREGISTERTIMESLICE_HDIFF_PREVHDIFF WHERE Previous_HashDiff <> HashDiff
		) as a
		
		
		--Update RecordEndDate and RecordDeletionFlag
		IF OBJECT_ID('staging.EXPECTED_DIMINSTALLATIONREGISTERRATE_Auto', 'U') IS NOT NULL
			DROP TABLE staging.EXPECTED_DIMINSTALLATIONREGISTERRATE_Auto

		CREATE TABLE staging.EXPECTED_DIMINSTALLATIONREGISTERRATE_Auto WITH 
		(
			DISTRIBUTION = HASH(Installation)
		)
		AS
		SELECT
			Base.InstallationId as Installation,
			Base.LogicalRegisterNumber,
			AB as RegisterValidFrom,
			Base.TimeSliceExpirationDate as RegisterValidTo,
			RateType as RegisterRateType,
			RateFactGroup as RegisterRateFactGroup,
			PREISKLA as RegisterPriceClass,
			RecordStartDate,
			CASE WHEN Base.RecordEndDate <> @DefaultCurrentDate THEN DATEADD(ns,-100, Base.RecordEndDate) 
		      WHEN PhysicalDel.InstallationId IS NOT NULL THEN PhysicalDel.LatestDeletionDate
		      ELSE Base.RecordEndDate 
			 END AS RecordEndDate,
			CASE WHEN PhysicalDel.InstallationId IS NOT NULL THEN 'X' ELSE NULL
			 END AS RecordDeletionIndicator,		
			--Extra Fields
			LOADDATETIME,
			DI_SEQUENCE_NUMBER,
			DI_OPERATION_TYPE,
			HashDiff,
			Previous_HashDiff
		FROM 
		 staging.RemoveDupAddEndDate_INSTALLATIONREGISTERRATE_Auto Base
		 LEFT JOIN 
	     (
		   SELECT
                   InstallationId, LogicalRegisterNumber, TimeSliceExpirationDate
				  ,MAX(RecordStartDate) AS LatestDeletionDate
		   FROM 
		          staging.RemoveDupAddEndDate_INSTALLATIONREGISTERRATE_Auto
		   WHERE
		         di_operation_type ='D'
		   GROUP BY
                  InstallationId, LogicalRegisterNumber, TimeSliceExpirationDate
		  ) AS PhysicalDel
		ON 
          BASE.InstallationId = PhysicalDel.InstallationId
		  AND BASE.LogicalRegisterNumber = PhysicalDel.LogicalRegisterNumber
		  AND BASE.TimeSliceExpirationDate = PhysicalDel.TimeSliceExpirationDate
		  AND BASE.RecordStartDate <= PhysicalDel.LatestDeletionDate
		
		END
		
		
		IF @ISEXECUTEVALIDATION = 1
		BEGIN
			PRINT @SRCAS + ' query: '
			PRINT @SATELLITEQUERY
			PRINT 'dim_InstallationRegisterRate query: '
			PRINT @DIMINSTALLATIONREGISTERRATEQUERY
			
			PRINT 'Executing except query between Satellite and dim_InstallationRegisterRate'
			EXECUTE (@SATELLITEQUERY + ' EXCEPT ' + @DIMINSTALLATIONREGISTERRATEQUERY)
			PRINT 'Executing except query between dim_InstallationRegisterRate and Satellite'
			EXECUTE (@DIMINSTALLATIONREGISTERRATEQUERY + ' EXCEPT ' + @SATELLITEQUERY)
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