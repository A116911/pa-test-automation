// Declare libraries

import java.util.Properties
import java.text.SimpleDateFormat
import java.util.Calendar
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col,lower,when,length,concat,lit}
val startTimeMillis = System.currentTimeMillis()

// ***************************************************************************************************************

    // ***************************************************************************************************************
    // Variable declaration
    // ***************************************************************************************************************
    try{
      dbutils.fs.refreshMounts()
      spark.table("parentTable_list").unpersist()      
    } catch {
        case e: Exception => println(e.getMessage())
    }
    
    val strUsername ="edplztstsa00"       // Source blob location(CSV and JSON)
    //val strSourceName = "crm"
    //val strRunDate = "2019-06-26"
    
    // CONNECTING TO THE LANDING ZONE
	spark.conf.set("fs.azure.account.key.edplztstsa00.blob.core.windows.net", dbutils.secrets.get(scope = "adbtstkv00", key = "edplztstsa00key"))


	// PATH TO THE TESTDATA CSV HAVING ALL TEST DATA
	var parenttablelistpath = "wasbs://test-parameters@edplztstsa00.blob.core.windows.net/table_list/csv_pqt/CSV_PQT_TestData.csv"
	
	// *****LOADING TABLE LIST*****
	var parentTable_list = spark.read.format("csv").option("header", "true").load(parenttablelistpath).toDF
	parentTable_list.createOrReplaceTempView("parentTable_list")
	spark.table("parentTable_list").cache    

	var pr_lst_DataSource = parentTable_list.select("system").collect().map(_(0)).toList
	var pr_lst_tables = parentTable_list.select("tablename").collect().map(_(0)).toList
	var pr_lst_RunDate = parentTable_list.select("rundate").collect().map(_(0)).toList	
	var pr_lst_DataClassification = parentTable_list.select("dataclassification").collect().map(_(0)).toList

    // **************************************************************************************************************************************************
    // Start of the Loop
    // **************************************************************************************************************************************************
    println("********** Execute data testing for CSV and Parquet files **********\n")
   
    for (iterator <- 0 until pr_lst_tables.length) 
    {
      //println("inside loop - 1")
      var strJsonFilePath = "wasbs://edp-etl-pipeline-inputs@" + strUsername + ".blob.core.windows.net/meta/typing/" + pr_lst_DataSource(iterator) + "/" + "inputTableName" + "/1/" + pr_lst_DataSource(iterator) + "_" + "inputTableName" + "_meta_1.json"

      var strParquetFilePath = "abfss://typed@edp" + pr_lst_DataClassification(iterator) + "tstsa00.dfs.core.windows.net/" + pr_lst_DataSource(iterator) + "/" + "inputTableName" + "/1/delta/" + pr_lst_RunDate(iterator) + "/*"

      val format = new SimpleDateFormat("dd/MM/yyyy")
      var runtime = format.format(Calendar.getInstance().getTime()).toString
      var fmtruntime = runtime.replace('/', '-')
      //var resultspath = "wasbs://test-parameters@" + strUsername + ".blob.core.windows.net/results/" + pr_lst_DataSource(iterator) + "/" + "inputTableName" + "/[" + fmtruntime + "]"
      
      
      // **************************************************************************************************************************************************
      // Connect to Azure Blob using credentials
      // **************************************************************************************************************************************************

      spark.conf.set("fs.azure.account.key.edp" + pr_lst_DataClassification(iterator) + "tstsa00.dfs.core.windows.net", dbutils.secrets.get(scope = "adbtstkv00", key = "edp" + pr_lst_DataClassification(iterator) + "tstsa00key"))
      spark.conf.set("fs.azure.createRemoteFileSystemDuringInitialization", "true")
      dbutils.fs.ls("abfss://typed@edp" + pr_lst_DataClassification(iterator) + "tstsa00.dfs.core.windows.net/")
      spark.conf.set("fs.azure.createRemoteFileSystemDuringInitialization", "false")
      
      // **************************************************************************************************************************************************
      // Read all Json files
      // **************************************************************************************************************************************************
      //println("before json read")
      val selcolumnNameJason = Seq("name", "type")
      var strsql = ""
      var strsqlUnion = ""
      //println("Current table list is : " +lst_tables)

        var strJsonFilePathnInner = strJsonFilePath.replaceAll("inputTableName", s"${pr_lst_tables(iterator)}")
        
        try {
          var df_jsonfile_loop = spark.read
            .option("parserLib", "univocity")
            .option("multiLine", "true")
            .json(strJsonFilePathnInner)
          var df_JsonTemp_stg = df_jsonfile_loop.select(selcolumnNameJason.map(name => col(name)): _*).filter("name !='di_sequence_number'").filter("name !='di_operation_type'").filter("name !='odq_changemode'").filter("name !='odq_entitycntr'").filter("name !='ds_update_ts'")
          var df_JsonTemp = df_JsonTemp_stg.withColumnRenamed("name", "fieldname").withColumnRenamed("type", "targettype")//.withColumnRenamed("length", "length")
          df_JsonTemp.createOrReplaceTempView("df_JsonTemp_" + pr_lst_tables(iterator))

          

          strsql = strsql.concat(" select * from df_JsonTemp_" + pr_lst_tables(iterator) + " union all ")
        } catch {
          case e: Exception => println(e.getMessage())
        }

      strsqlUnion = strsql.substring(0, strsql.length - 10)

      var df_json = spark.sql(strsqlUnion)
      spark.catalog.dropTempView("df_JsonTemp_" + pr_lst_tables(iterator))

      //df_json.show()
      //spark.catalog.dropTempView("df_JsonTemp_" + pr_lst_tables(iterator))

      //println("after json read")
      // **************************************************************************************************************************************************
      // Create final data frame for Json data
      // **************************************************************************************************************************************************

      var dd03l_json_data = df_json.withColumn("fieldname", lower(col("fieldname"))).withColumn("targettype", lower(col("targettype")))//.withColumn("length", lower(col("length")))
      dd03l_json_data.createOrReplaceTempView("dd03l_json_data")
      spark.table("dd03l_json_data").cache
      //println("Metadata from JSON file:")
      //dd03l_json_data.show(dd03l_json_data.count.toInt, false)
      
      
    // **************************************************************************************************************************************************
    // Read all Parquet files
    // **************************************************************************************************************************************************
    var df_Parquet_metadata_stg = spark.emptyDataFrame
    var df_Parquet_data = Seq.empty[(String, String)].toDF("columnname", "datatype")
    strsql = ""
    strsqlUnion = ""

    //Read Parquet mete data files
    //for (i <- 0 until lst_tables.length) {
      var strParquetFilePathInner = strParquetFilePath.replaceAll("inputTableName", s"${pr_lst_tables(iterator)}")
      try {

        var df_Parquet_Temp = spark.read.parquet(strParquetFilePathInner)

        df_Parquet_Temp.createOrReplaceTempView("df_Parque_Temp_" + pr_lst_tables(iterator))
        df_Parquet_metadata_stg = spark.sql("DESCRIBE table df_Parque_Temp_" + pr_lst_tables(iterator))
        df_Parquet_metadata_stg.createOrReplaceTempView("df_Parquet_Temp_meta")
        //println("Parquet meta data:")
        //df_Parquet_metadata_stg.show()

        //df_Parquet_data = df_Parquet_data.union(spark.sql("select '" + pr_lst_tables(iterator) + "' as tablename, a.* from df_Parquet_Temp_meta a"))
        df_Parquet_data = df_Parquet_data.union(spark.sql("select col_name,data_type from df_Parquet_Temp_meta"))
        
        //df_Parquet_data.show()
        
      } catch {
        case e: Exception => println(e.getMessage())
      }

      spark.catalog.dropTempView("df_Parque_Temp_" + pr_lst_tables(iterator))
    //println("Metadata from Parquet file:")
    //df_Parquet_data.show(df_Parquet_data.count.toInt, false)
    // **************************************************************************************************************************************************
    // Create final data frame for Parquet data
    // **************************************************************************************************************************************************
    df_Parquet_data.createOrReplaceTempView("df_Parquet_data")
    spark.table("df_Parquet_data").cache

    

    var df_di_parquet = Seq.empty[(String, Long, String)].toDF("tablename", "fail_count", "data_value_status")
    
    // **************************************************************************************************************************************************
    // CSV and Parqut Metadata Comparison
    // **************************************************************************************************************************************************  
      
      try {
        
        df_di_parquet = df_di_parquet.union(spark.sql("select '" + pr_lst_tables(iterator) + "' as tablename,count(*) as mis_natch_count, case when count(*) != 0 then 'FAIL' else 'PASS' end as data_value_status from (select fieldname,case when targettype='integer' then 'int' when targettype='time' then 'string' else targettype end as targettype from dd03l_json_data minus select columnname, case when datatype like 'decimal%' then 'decimal' else datatype end as datatype from df_Parquet_data where columnname not in ('_filename','_index','_errors','_ts') )"))
       
       println("Testing for table : " + pr_lst_tables(iterator))
       println(" ")
        
       
       println("DD03l meta data value:")
       dd03l_json_data.show(dd03l_json_data.count.toInt, false)
       println("Parquet meta data value:")
       df_Parquet_data.show(df_Parquet_data.count.toInt, false)
       println("Execute mete data value testing (Json - Parquet )")
       df_di_parquet.show(df_di_parquet.count.toInt, false)
       df_di_parquet.createOrReplaceTempView("df_di_parquet")

       // **************************************************************************************************************************************************
       // CSV and Parqut Count Comparison
       // **************************************************************************************************************************************************
        

        var df_csv = spark.read.format("csv").option("header", "false").option("delimiter", "\u0001").load("wasbs://" + pr_lst_DataSource(iterator) + "@" + strUsername + ".blob.core.windows.net/" + pr_lst_tables(iterator) + "/1/extract_date=" + pr_lst_RunDate(iterator) + "/*.csv.bz2")
                
                
        //df_csv = df_csv.withColumn("_c11",when(length(col("_c11"))<3,concat(lit("00"), col("_c11"))).otherwise(col("_c11")))
        //df_csv = df_csv.withColumn("_c17",when(length(col("_c17"))<12,concat(lit("00000000000"), col("_c17"))).otherwise(col("_c17")))
       
        df_csv.createOrReplaceTempView("csv")

        val csvCount = df_csv.count()

        var df_pqt = spark.read.parquet(strParquetFilePathInner) 
        df_pqt = df_pqt.drop("monotonically_increasing_id","filename")        
        

        df_pqt.createOrReplaceTempView("pqt")

        val pqtCount = df_pqt.count()

        //pqtCount = sqlContext.sql("SELECT count(*) FROM pqt")

        println("CSV Count for " + pr_lst_tables(iterator) + " is : " + csvCount + " records ")

        println("PQT Count for " + pr_lst_tables(iterator) + " is : " + pqtCount + " records ")
        println(" ")

        if ((csvCount - pqtCount) == 0){
          println("Data count matches for both CSV and Parquet table --- PASS.")
        } else{
          println("there is a mismatch in CSV and Parquet data count --- FAIL.")
        }
        println("*******************************************")
        

        // **************************************************************************************************************************************************
        // CSV and Parqut Dataset Comparison
        // **************************************************************************************************************************************************
        

        var df_tempCsv = df_csv.drop()
        
        
        //val df_tempPqt = df_pqt.drop("di_sequence_number","di_operation_type","_monotonically_increasing_id","_filename","_errors")
        val df_tempPqt = df_pqt.drop()

        val df_resultCsv = df_tempCsv.except(df_tempPqt)

        val df_resultPqt = df_tempPqt.except(df_tempCsv)

        df_resultCsv.createOrReplaceTempView("ResultCsv")

        df_resultPqt.createOrReplaceTempView("ResultPqt")

        val pqtDataCount = df_resultPqt.count()   // spark.sql("SELECT count(*) FROM ResultPqt" )

        val csvDataCount = df_resultCsv.count()   // spark.sql("SELECT count(*) FROM ResultCsv" )
        
        println(" ")
        println("Execute data comparison between CSV and Parquet ")
        println(" ")

        println("CSV Vs PQT comparison output:")
        spark.sql("SELECT * FROM ResultCsv" ).show()
        println("Data mismatch count in between CSV and PQT :" + csvDataCount + " records ")

        println(" ")

        println("PQT Vs CSV comparison output:")
        spark.sql("SELECT * FROM ResultPqt" ).show()
        println("Data mismatch count in between PQT and CSV :" + pqtDataCount + " records ")

        println("\n#################################################################################\n")

      } catch {
        case e: Exception => println(e.getMessage())
      }

    }
    // **************************************************************************************************************************************************
    // End of the Loop
    // **************************************************************************************************************************************************

    spark.table("parentTable_list").unpersist()

    println("Script Execution Duration : " + (System.currentTimeMillis() - startTimeMillis) / 1000 + " seconds \n") 

//End