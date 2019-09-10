/****** Object:  StoredProcedure staging.validate_full_load_diminstallation    Script Date: 22/08/2019 4:26:58 PM ******/
IF OBJECT_ID('staging.validate_full_load_diminstallation', 'P') IS NOT NULL
	DROP PROCEDURE staging.validate_full_load_diminstallation
GO

CREATE PROCEDURE staging.validate_full_load_diminstallation @SRCAS NVARCHAR(MAX)
	,@ISCREATEEXPECTEDDIMTABLE BIT
	,@ISEXECUTEVALIDATION BIT
	,@ISDROPTEMPTABLE BIT
AS
--Stored proc to test Full Load:  SAP ISU EANL vs dim_installation table
BEGIN
	BEGIN TRY
		DECLARE @SATELLITEQUERY NVARCHAR(MAX) -- Store final satellite select query
		DECLARE @INSTALLATIONDIMQUERY NVARCHAR(MAX) -- Store final dim_installation select query
		DECLARE @ALLSTAGINGTABLENAMES NVARCHAR(MAX)
		DECLARE @STAGINGSCHEMANAME NVARCHAR(MAX)
		DECLARE @STAGINGTABLENAME NVARCHAR(MAX)

		SET @STAGINGSCHEMANAME = 'staging'
		SET @STAGINGTABLENAME = 'stg_isu_eanl'
		SET @SATELLITEQUERY = '(select 
							InstallationId as ANLAGE,
							ANLART,
							DEREGSTAT,
							HAZARD,
							DOGCODE,
							LOEVM,
							PremiseId as VSTELLE
							,RecordStartDate
							,RecordEndDate
							--,di_sequence_number
							--,HashDiff
							--,Previous_HashDiff
						from staging.TMPAUTO_PREVHASHDIFF_REMOVEDUPL_sat_ISU_Installation )'
		SET @INSTALLATIONDIMQUERY = '(select 
									Installation,
									InstallationType,
									DeregulationStatus,
									Hazard,
									DogCode,
									DeletionIndicator,
									Premise
									--RecordDeletionIndicator,
									,RecordStartDate
									,RecordEndDate
								from commercial.dim_Installation )'

		IF @ISCREATEEXPECTEDDIMTABLE = 1
		BEGIN
			IF OBJECT_ID('staging.TMPAUTO_sat_ISU_Installation', 'U') IS NOT NULL
				DROP TABLE staging.TMPAUTO_sat_ISU_Installation

			IF (@SRCAS = 'SATELLITE')
			BEGIN
				CREATE TABLE staging.TMPAUTO_sat_ISU_Installation
					WITH (DISTRIBUTION = HASH (InstallationId)) AS

				--TODO: Deletes to be handled
				--Calculate Hash of required columns in satellite table
				SELECT InstallationId
					,ANLART
					,DEREGSTAT
					,HAZARD
					,DOGCODE
					,LOEVM
					,PremiseId
					,sat.di_sequence_number
					,sat.ds_update_ts AS RecordStartDate
					,sat.di_operation_type
					,CONVERT(BINARY (32), HASHBYTES('SHA2_256', CONCAT (
								LTRIM(RTRIM(ISNULL(sat.ANLART, '')))
								,'|'
								,LTRIM(RTRIM(ISNULL(sat.DEREGSTAT, '')))
								,'|'
								,LTRIM(RTRIM(ISNULL(sat.HAZARD, '')))
								,'|'
								,LTRIM(RTRIM(ISNULL(sat.DOGCODE, '')))
								,'|'
								,LTRIM(RTRIM(ISNULL(sat.LOEVM, '')))
								,'|'
								,LTRIM(RTRIM(ISNULL(sat.PremiseId, '')))
								,'|'
								))) AS HashDiff
					,ROW_NUMBER() OVER (
						PARTITION BY sat.InstallationId
						,sat.DS_UPDATE_TS ORDER BY sat.DI_SEQUENCE_NUMBER DESC
							,sat.LOADDATETIME DESC
						) AS RN
				FROM dv_commercial.sat_ISU_Installation sat
				WHERE sat.di_operation_type <> 'D'
			END
			ELSE IF (UPPER(@SRCAS) = 'STAGING')
			BEGIN
				IF OBJECT_ID('staging.TMPAUTO_ALLSTAGINGDATA', 'U') IS NOT NULL
					DROP TABLE staging.TMPAUTO_ALLSTAGINGDATA

				SET @ALLSTAGINGTABLENAMES = (
						SELECT string_agg(cast(CONCAT (
										'select * from '
										,ssc.name
										,'.'
										,st.name
										,' union all '
										) AS VARCHAR(MAX)), '') AS schema_table
						FROM sys.tables AS st
						INNER JOIN sys.objects so ON st.object_id = so.object_id
						INNER JOIN sys.schemas ssc ON ssc.schema_id = st.schema_id
						WHERE ssc.name = @STAGINGSCHEMANAME
							AND st.name LIKE ('%' + @STAGINGTABLENAME + '[_]%')
						)
				SET @ALLSTAGINGTABLENAMES = LEFT(@ALLSTAGINGTABLENAMES, LEN(@ALLSTAGINGTABLENAMES) - LEN(' union all '))

				PRINT 'Staging table union query: ' + @ALLSTAGINGTABLENAMES

				SET @ALLSTAGINGTABLENAMES = 'CREATE TABLE staging.TMPAUTO_ALLSTAGINGDATA 
											WITH
											(
												DISTRIBUTION =  HASH (anlage)
											) AS ' + @ALLSTAGINGTABLENAMES

				EXECUTE (@ALLSTAGINGTABLENAMES)

				CREATE TABLE staging.TMPAUTO_sat_ISU_Installation
					WITH (DISTRIBUTION = HASH (InstallationId)) AS

				SELECT all_eanl_tables.ANLAGE AS InstallationId
					,all_eanl_tables.ANLART AS ANLART
					,all_eanl_tables.DEREGSTAT AS DEREGSTAT
					,all_eanl_tables.HAZARD AS HAZARD
					,all_eanl_tables.DOGCODE AS DOGCODE
					,all_eanl_tables.LOEVM AS LOEVM
					,all_eanl_tables.VSTELLE AS PremiseId
					,all_eanl_tables.di_sequence_number
					,all_eanl_tables.ds_update_ts AS RecordStartDate
					,all_eanl_tables.di_operation_type
					,CONVERT(BINARY (32), HASHBYTES('SHA2_256', CONCAT (
								LTRIM(RTRIM(ISNULL(all_eanl_tables.ANLART, '')))
								,'|'
								,LTRIM(RTRIM(ISNULL(all_eanl_tables.DEREGSTAT, '')))
								,'|'
								,LTRIM(RTRIM(ISNULL(all_eanl_tables.HAZARD, '')))
								,'|'
								,LTRIM(RTRIM(ISNULL(all_eanl_tables.DOGCODE, '')))
								,'|'
								,LTRIM(RTRIM(ISNULL(all_eanl_tables.LOEVM, '')))
								,'|'
								,LTRIM(RTRIM(ISNULL(all_eanl_tables.VSTELLE, '')))
								,'|'
								))) AS HashDiff
					,ROW_NUMBER() OVER (
						PARTITION BY all_eanl_tables.ANLAGE
						,all_eanl_tables.DS_UPDATE_TS ORDER BY all_eanl_tables.DI_SEQUENCE_NUMBER DESC
							,all_eanl_tables.LOADDATETIME DESC
						) AS RN
				FROM (
					SELECT *
					FROM staging.TMPAUTO_ALLSTAGINGDATA
					) AS all_eanl_tables
				WHERE all_eanl_tables.di_operation_type <> 'D'
			END

			---- Add Previous Hashdiff (one change before the current) to the current record 
			IF OBJECT_ID('staging.TMPAUTO_PREVHASHDIFF_sat_ISU_Installation', 'U') IS NOT NULL
				DROP TABLE staging.TMPAUTO_PREVHASHDIFF_sat_ISU_Installation

			CREATE TABLE staging.TMPAUTO_PREVHASHDIFF_sat_ISU_Installation
				WITH (DISTRIBUTION = HASH (InstallationId)) AS

			SELECT InstallationId
				,ANLART
				,DEREGSTAT
				,HAZARD
				,DOGCODE
				,LOEVM
				,PremiseId
				,di_sequence_number
				,RecordStartDate
				,di_operation_type
				,HashDiff
				,LAG(HashDiff, 1) OVER (
					PARTITION BY InstallationId ORDER BY RecordStartDate
						,di_sequence_number
					) AS Previous_HashDiff
			FROM staging.TMPAUTO_sat_ISU_Installation
			WHERE RN = '1'

			----Remove records with same date in ds_update_ts and take record with max di_sequence_number
			IF OBJECT_ID('staging.TMPAUTO_PREVHASHDIFF_FILTRSAMEDATE_sat_ISU_Installation', 'U') IS NOT NULL
				DROP TABLE staging.TMPAUTO_PREVHASHDIFF_FILTRSAMEDATE_sat_ISU_Installation

			CREATE TABLE staging.TMPAUTO_PREVHASHDIFF_FILTRSAMEDATE_sat_ISU_Installation
				WITH (DISTRIBUTION = HASH (InstallationId)) AS

			SELECT TEMP.InstallationId
				,TEMP.ANLART
				,TEMP.DEREGSTAT
				,TEMP.HAZARD
				,TEMP.DOGCODE
				,TEMP.LOEVM
				,TEMP.PremiseId
				,TEMP.di_sequence_number
				,TEMP.RecordStartDate
				,TEMP.di_operation_type
				,TEMP.HashDiff
				,TEMP.Previous_HashDiff
			FROM staging.TMPAUTO_PREVHASHDIFF_sat_ISU_Installation TEMP
			RIGHT JOIN (
				SELECT InstallationId
					,max(di_sequence_number) AS max_di_sequence_number
					,max(RecordStartDate) AS max_RecordStartDate
				FROM staging.TMPAUTO_PREVHASHDIFF_sat_ISU_Installation
				GROUP BY InstallationId
					,RecordStartDate
				) AS max_disq_recstrtdt ON TEMP.InstallationId = max_disq_recstrtdt.InstallationId
				AND TEMP.di_sequence_number = max_disq_recstrtdt.max_di_sequence_number
				AND TEMP.RecordStartDate = max_disq_recstrtdt.max_RecordStartDate

			---- Remove duplicate entries and add enddate --------------
			IF OBJECT_ID('staging.TMPAUTO_PREVHASHDIFF_REMOVEDUPL_sat_ISU_Installation', 'U') IS NOT NULL
				DROP TABLE staging.TMPAUTO_PREVHASHDIFF_REMOVEDUPL_sat_ISU_Installation

			CREATE TABLE staging.TMPAUTO_PREVHASHDIFF_REMOVEDUPL_sat_ISU_Installation
				WITH (DISTRIBUTION = HASH (InstallationId)) AS

			SELECT InstallationId
				,ANLART
				,DEREGSTAT
				,HAZARD
				,DOGCODE
				,LOEVM
				,PremiseId
				,di_sequence_number
				,RecordStartDate
				,LEAD(DATEADD(ns, - 100, RecordStartDate), 1, '9999-12-31 00:00:00.000') OVER (
					PARTITION BY InstallationId ORDER BY RecordStartDate
						,di_sequence_number
					) AS RecordEndDate
				,di_operation_type
				,HashDiff
				,Previous_HashDiff
			FROM (
				SELECT *
				FROM staging.TMPAUTO_PREVHASHDIFF_FILTRSAMEDATE_sat_ISU_Installation
				WHERE Previous_HashDiff IS NULL
				
				UNION ALL
				
				SELECT *
				FROM staging.TMPAUTO_PREVHASHDIFF_FILTRSAMEDATE_sat_ISU_Installation
				WHERE Previous_HashDiff <> HashDiff
				) AS Base
		END

		IF @ISEXECUTEVALIDATION = 1
		BEGIN
			PRINT @SRCAS + ' query: '
			PRINT @SATELLITEQUERY
			PRINT 'DIM_INSTALLATION query: '
			PRINT @INSTALLATIONDIMQUERY
			PRINT 'Executing except query between ' + @SRCAS + ' and DIM_INSTALLATION table'

			EXECUTE (@SATELLITEQUERY + ' EXCEPT ' + @INSTALLATIONDIMQUERY)

			PRINT 'Executing except query between DIM_INSTALLATION and ' + @SRCAS

			EXECUTE (@INSTALLATIONDIMQUERY + ' EXCEPT ' + @SATELLITEQUERY)
		END

		IF @ISDROPTEMPTABLE = 1
		BEGIN
			PRINT 'DROP TEMP TABLE FLAG IS SET TO: ' + CONVERT(VARCHAR(1), @ISDROPTEMPTABLE)
			IF OBJECT_ID('staging.TMPAUTO_ALLSTAGINGDATA', 'U') IS NOT NULL
			BEGIN
				DROP TABLE staging.TMPAUTO_ALLSTAGINGDATA
				PRINT 'DROPPED staging.TMPAUTO_ALLSTAGINGDATA'
			END
			
			IF OBJECT_ID('staging.TMPAUTO_sat_ISU_Installation', 'U') IS NOT NULL
			BEGIN
				DROP TABLE staging.TMPAUTO_sat_ISU_Installation

				PRINT 'DROPPED staging.TMPAUTO_sat_ISU_Installation'
			END

			IF OBJECT_ID('staging.TMPAUTO_PREVHASHDIFF_sat_ISU_Installation', 'U') IS NOT NULL
			BEGIN
				DROP TABLE staging.TMPAUTO_PREVHASHDIFF_sat_ISU_Installation

				PRINT 'DROPPED staging.TMPAUTO_PREVHASHDIFF_sat_ISU_Installation'
			END

			IF OBJECT_ID('staging.TMPAUTO_PREVHASHDIFF_FILTRSAMEDATE_sat_ISU_Installation', 'U') IS NOT NULL
			BEGIN
				DROP TABLE staging.TMPAUTO_PREVHASHDIFF_FILTRSAMEDATE_sat_ISU_Installation

				PRINT 'DROPPED staging.TMPAUTO_PREVHASHDIFF_FILTRSAMEDATE_sat_ISU_Installation'
			END

			IF OBJECT_ID('staging.TMPAUTO_PREVHASHDIFF_REMOVEDUPL_sat_ISU_Installation', 'U') IS NOT NULL
			BEGIN
				DROP TABLE staging.TMPAUTO_PREVHASHDIFF_REMOVEDUPL_sat_ISU_Installation

				PRINT 'DROPPED staging.TMPAUTO_PREVHASHDIFF_REMOVEDUPL_sat_ISU_Installation'
			END
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
GO

