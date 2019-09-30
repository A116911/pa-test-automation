/****** Object:  StoredProcedure [staging].[validate_contract_discount_history]    Script Date: 26/09/2019 12:41:59 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
ALTER PROC [staging].[validate_contract_discount_history] @SRCAS [NVARCHAR](MAX),@ISCREATEEXPECTEDDIMTABLE [BIT],@ISEXECUTEVALIDATION [BIT],@ISDROPTEMPTABLE [BIT] AS
BEGIN
	BEGIN TRY
		DECLARE @SATELLITEQUERY NVARCHAR(MAX) -- Store final satellite select query
		DECLARE @CONTRACTDISCOUNTHISTDIMQUERY NVARCHAR(MAX) -- Store final dim_installation select query
		DECLARE @ALLSTAGINGTABLENAMES NVARCHAR(MAX)
		DECLARE @STAGINGSCHEMANAME NVARCHAR(MAX)
		DECLARE @STAGINGTABLENAME NVARCHAR(MAX)

		SET @STAGINGSCHEMANAME = 'staging'
		SET @STAGINGTABLENAME = 'stg_isu_eanl'
		SET @SATELLITEQUERY = '(select 
										Contract,
										DiscountOperand,
										ValidFrom,
										ValidTo,
										Season,
										EffectiveDateConsecutiveNumber,
										DiscountValueType,
										DiscountDescription,
										DiscountValue
									from staging.TMPAUTO_CONTRACTINSTALLATION_DISCOUNT )'
									
		SET @CONTRACTDISCOUNTHISTDIMQUERY = '(select 
										Contract,
										DiscountOperand,
										ValidFrom,
										ValidTo,
										Season,
										EffectiveDateConsecutiveNumber,
										DiscountValueType,
										DiscountDescription,
										DiscountValue
									from commercial.dim_ContractDiscountHistory_cr)'

		IF @ISCREATEEXPECTEDDIMTABLE = 1
		BEGIN
			
			
			IF (@SRCAS = 'SATELLITE')
			BEGIN
			
				--Pick latest Installations from ETTIFN
				IF OBJECT_ID('staging.TMPAUTO_sat_ISU_InstallationOperandTimeSlice', 'U') IS NOT NULL
					DROP TABLE staging.TMPAUTO_sat_ISU_InstallationOperandTimeSlice
				
				CREATE TABLE staging.TMPAUTO_sat_ISU_InstallationOperandTimeSlice
					WITH (DISTRIBUTION = HASH (InstallationId)) AS
				
				SELECT * FROM				
				(SELECT *
				,ROW_NUMBER() OVER (PARTITION BY InstallationId	
											,OperandCode
											,[Season]
											,[EffectiveDateConsecutiveNumber]
											,[TimeSliceEffectiveDate]
								ORDER BY     DS_UPDATE_TS DESC
											,DI_SEQUENCE_NUMBER DESC
											,LOADDATETIME DESC
							) AS RN
				FROM [dv_commercial].[sat_ISU_InstallationOperandTimeSlice]) as ettifn
				WHERE ettifn.inaktiv IS NULL
				AND ettifn.DI_OPERATION_TYPE <> 'D'
				AND ettifn.RN = 1 
				
				--Pick latest Contracts from EVER
				IF OBJECT_ID('staging.TMPAUTO_sat_ISU_ContractInstallation', 'U') IS NOT NULL
					DROP TABLE staging.TMPAUTO_sat_ISU_ContractInstallation
					
				CREATE TABLE staging.TMPAUTO_sat_ISU_ContractInstallation
					WITH (DISTRIBUTION = HASH (CONTRACTNUMBER)) AS	
					
					SELECT * FROM
					(SELECT 
					CONTRACTNUMBER
					,INSTALLATIONID
					,LOEVM
					,AUSZDAT
					,EINZDAT
					,DI_OPERATION_TYPE
					,ROW_NUMBER() OVER (PARTITION BY CONTRACTNUMBER
			                     ORDER BY     DS_UPDATE_TS DESC
								             ,DI_SEQUENCE_NUMBER DESC
											 ,LOADDATETIME DESC
										) AS RN
					FROM  [dv_commercial].[sat_ISU_ContractInstallation]) ever
					WHERE ever.LOEVM IS NULL
					AND ever.DI_OPERATION_TYPE <> 'D'
					AND ever.RN = 1
					
				--JOIN LATEST RECORDS FROM ETTIFN AND EVER				
				IF OBJECT_ID('staging.TMPAUTO_JOIN_CONTRACT_INSTALLATION', 'U') IS NOT NULL
					DROP TABLE staging.TMPAUTO_JOIN_CONTRACT_INSTALLATION
				CREATE TABLE staging.TMPAUTO_JOIN_CONTRACT_INSTALLATION
					WITH (DISTRIBUTION = HASH (Contract)) AS
					SELECT
					ever.ContractNumber AS Contract ---pk					
					,ettifn.[TimeSliceEffectiveDate] as ValidFrom ----pk
					,ettifn.[BIS] as ValidTo
					,ettifn.Season					
					,ettifn.EffectiveDateConsecutiveNumber
					,ettifn.OPERANDCODE
					,CASE WHEN ettifn.WERT1 IS NOT NULL THEN ettifn.WERT1 
						ELSE ettifn.STRING1
					END AS DiscountValue
					FROM
					staging.TMPAUTO_sat_ISU_ContractInstallation ever INNER JOIN 
					staging.TMPAUTO_sat_ISU_InstallationOperandTimeSlice ettifn
					ON 										
					ever.INSTALLATIONID = ettifn.INSTALLATIONID					
					AND ever.AUSZDAT >= ettifn.[TimeSliceEffectiveDate]
					AND ever.EINZDAT <= ettifn.BIS
					
					
				--GET DiscountOperand, DiscountValueType, DiscountDescription
				IF OBJECT_ID('staging.TMPAUTO_DISCOUNT_CODES', 'U') IS NOT NULL
					DROP TABLE staging.TMPAUTO_DISCOUNT_CODES
				
				CREATE TABLE staging.TMPAUTO_DISCOUNT_CODES
					WITH (DISTRIBUTION = HASH (DiscountOperand)) AS					
					SELECT
						distinct SAT_OP.OPERANDCODE AS DiscountOperand
						,SAT_OP.OPTYP AS DiscountValueType
						,OP_TEXT.TEXT30 AS DiscountDescription
					FROM [dv_open].[sat_ISU_Operand] AS SAT_OP					
					
					INNER JOIN							
					(					
					SELECT EIN01 AS OPERAND
					FROM [dv_commercial].[ref_RateSteps]
					WHERE CHNG_SIGN = 'X'

					UNION ALL	
	
					SELECT EIN02 AS OPERAND
					FROM [dv_commercial].[ref_RateSteps]
					WHERE CHNG_SIGN = 'X'
					) AS DISCOUNT_OP 
					ON 
					SAT_OP.OPERANDCODE = DISCOUNT_OP.OPERAND					
					
					LEFT JOIN [dv_open].[ref_OperandText] AS OP_TEXT					
					ON 
					OP_TEXT.Operand = SAT_OP.OperandCode
					AND SPRAS = 'E'
					WHERE 
					SAT_OP.OPTYP IN ('FACTOR', 'AMOUNT') and
					SAT_OP.DI_OPERATION_TYPE <> 'D'
					
					
				--ADD DiscountOperand, DiscountValueType, DiscountDescription
				IF OBJECT_ID('staging.TMPAUTO_CONTRACTINSTALLATION_DISCOUNT', 'U') IS NOT NULL
					DROP TABLE staging.TMPAUTO_CONTRACTINSTALLATION_DISCOUNT
				CREATE TABLE staging.TMPAUTO_CONTRACTINSTALLATION_DISCOUNT
					WITH (DISTRIBUTION = HASH (Contract)) AS
					SELECT 
					contract_instal.Contract
					,dis_codes.DiscountOperand
					,contract_instal.ValidFrom
					,contract_instal.ValidTo
					,contract_instal.Season					
					,contract_instal.EffectiveDateConsecutiveNumber
					,dis_codes.DiscountValueType
					,dis_codes.DiscountDescription
					,contract_instal.DiscountValue
					FROM
					staging.TMPAUTO_JOIN_CONTRACT_INSTALLATION contract_instal
					INNER JOIN staging.TMPAUTO_DISCOUNT_CODES dis_codes
					ON contract_instal.OPERANDCODE = dis_codes.DiscountOperand				
				
		
		
			END
		
		END
		
		IF @ISEXECUTEVALIDATION = 1
		BEGIN
			PRINT @SRCAS + ' query: '
			PRINT @SATELLITEQUERY
			PRINT 'dim_ContractDiscountHistory_cr query: '
			PRINT @CONTRACTDISCOUNTHISTDIMQUERY
			PRINT 'Executing except query between ' + @SRCAS + ' and dim_ContractDiscountHistory_cr table'

			EXECUTE (@SATELLITEQUERY + ' EXCEPT ' + @CONTRACTDISCOUNTHISTDIMQUERY)

			PRINT 'Executing except query between dim_ContractDiscountHistory_cr and ' + @SRCAS

			EXECUTE (@CONTRACTDISCOUNTHISTDIMQUERY + ' EXCEPT ' + @SATELLITEQUERY)
		END
		
		IF @ISDROPTEMPTABLE = 1
		BEGIN
			PRINT 'DROP TEMP TABLE FLAG IS SET TO: ' + CONVERT(VARCHAR(1), @ISDROPTEMPTABLE)
			IF OBJECT_ID('staging.TMPAUTO_sat_ISU_InstallationOperandTimeSlice', 'U') IS NOT NULL
			BEGIN
				DROP TABLE staging.TMPAUTO_sat_ISU_InstallationOperandTimeSlice
				PRINT 'DROPPED staging.TMPAUTO_sat_ISU_InstallationOperandTimeSlice'
			END
			
			IF OBJECT_ID('staging.TMPAUTO_sat_ISU_ContractInstallation', 'U') IS NOT NULL
			BEGIN
				DROP TABLE staging.TMPAUTO_sat_ISU_ContractInstallation
				PRINT 'DROPPED staging.TMPAUTO_sat_ISU_ContractInstallation'
			END

			IF OBJECT_ID('staging.TMPAUTO_JOIN_CONTRACT_INSTALLATION', 'U') IS NOT NULL
			BEGIN
				DROP TABLE staging.TMPAUTO_JOIN_CONTRACT_INSTALLATION
				PRINT 'DROPPED staging.TMPAUTO_JOIN_CONTRACT_INSTALLATION'
			END

			IF OBJECT_ID('staging.TMPAUTO_DISCOUNT_CODES', 'U') IS NOT NULL
			BEGIN
				DROP TABLE staging.TMPAUTO_DISCOUNT_CODES
				PRINT 'DROPPED staging.TMPAUTO_PREVHASHDIFF_FILTRSAMEDATE_sat_ISU_Installation'
			END

			IF OBJECT_ID('staging.TMPAUTO_CONTRACTINSTALLATION_DISCOUNT', 'U') IS NOT NULL
			BEGIN
				DROP TABLE staging.TMPAUTO_CONTRACTINSTALLATION_DISCOUNT
				PRINT 'DROPPED staging.TMPAUTO_CONTRACTINSTALLATION_DISCOUNT'
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