// Declare libraries

import java.util.Properties
import java.text.SimpleDateFormat
import java.util.Calendar
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col,lower,when,length,concat,lit,trim}
import org.apache.spark.sql.{DataFrame,Column,Dataset,Row}
import org.apache.spark.sql.types.DataTypes
import scala.collection.immutable.List

val startTimeMillis = System.currentTimeMillis()

//This function will replace empty string with null string in all columns in a dataframe
def replaceEmptyStringsWithNull_allColumns(df: DataFrame): DataFrame = {
  df.schema.foldLeft(df)(
    (current, field) =>
      field.dataType match {
        case DataTypes.StringType =>
          current.withColumn(
            field.name,
            when(length(col(field.name)) === 0, lit(null: String)).otherwise(col(field.name))
          )
        case _ => current
      }
  )
} 

//This function will append zeroes to a column in a dataframe upto the columnLength
//NewDataframe with modidied column will be returned
def appendZeroes(df: DataFrame, columnName: String, columnLength: Int) : DataFrame = {
  var newDf: DataFrame = null
  var tempDf: DataFrame = df
  for( counter <- 1 to columnLength){
    tempDf = tempDf.withColumn(columnName,when(length(col(columnName))<counter,concat(lit("0"), col(columnName))).otherwise(col(columnName)))
  } 
  newDf = tempDf
  return newDf
}

//This function will trim a single column in a dataframe
def trimColumn(df: DataFrame, colName: String): DataFrame = {    
  var newDf = df.withColumn(colName, trim(col(colName)))
  return newDf
}

//This function will trim all columns in a dataframe
def trimAllColumns(df: DataFrame): DataFrame = {  
  var newDf = df.columns.foldLeft(df) { (memoDF, colName) =>
                      memoDF.withColumn(
                        colName,      
                        trim(col(colName))
                      )
  }  
  return newDf
}

//This function will replace empty string with null string in a single column in a dataframe
def replaceEmptyStringsWithNull_perColumn(df: DataFrame, colName: String): DataFrame = {    
  var newDf = df.withColumn(colName, when(length(col(colName)) === 0, lit(null: String)).otherwise(col(colName)))
  return newDf
}

//This function will drop a column from a dataframe
def dropColumn(df: DataFrame, colName: String): DataFrame = {    
  var newDf: DataFrame = null
  newDf = df.drop(colName)  
  return newDf
}

//This function will drop last column from a dataframe
def dropLastColumn(df: DataFrame): DataFrame = {    
  var newDf: DataFrame = null
  newDf = df.drop(df.columns.last)  
  return newDf
}

//This function will take dataframe that needs to be cleaned and a dataframe of known issues
//It will clean the dataframe as per the known issues and return a cleaned dataframe
def fixKnownDataIssues(tobecleaned_df: DataFrame, kwnissues_orgn_colnme_func_df: DataFrame): DataFrame = {
  //if(kwnissues_orgn_colnme_func_df.limit(1).collect().isEmpty)
  var column_list: List[String] = kwnissues_orgn_colnme_func_df.select("column_name").map(_.getString(0)).collect().toList
  var function_names = kwnissues_orgn_colnme_func_df.select("function").collect().map(_.getString(0)).toList
  var newDf: DataFrame = null
  var tempDf: DataFrame = tobecleaned_df
  
  for(counter <- 0 until column_list.size){    
    var column_name_arry = column_list(counter).split(";")
    var function_name = function_names(counter)
    
    for(column_name <- column_name_arry) {      
      newDf = determineActionAndExecute(tempDf, column_name, function_name)
      tempDf = newDf
    }      
  }
  newDf = tempDf
  return newDf
}


//This function will take the dataframe that needs to be cleaned, column name that needs cleaning and function to be performed on the column
def determineActionAndExecute(tobecleaned_df: DataFrame, column_name: String, func_name: String): DataFrame = { 
  var newDf: DataFrame = null
  
  if(func_name.toLowerCase().trim().equals("drop_columns")){
    newDf = dropColumn(tobecleaned_df, column_name)
  }else if(func_name.toLowerCase().trim().equals("trim_columns")){
    newDf = trimColumn(tobecleaned_df, column_name)
  }else if(func_name.toLowerCase().trim().equals("replace_emptystring_with_null")){
    newDf = replaceEmptyStringsWithNull_perColumn(tobecleaned_df, column_name)
  }else if(func_name.toLowerCase().trim().equals("drop_last_column")){
    newDf = dropLastColumn(tobecleaned_df)
  }else if(func_name.toLowerCase().trim().contains("append_zeroes_upto_")){    
    var lengthOfField:Int = (func_name.replaceAll("append_zeroes_upto_","")).toInt
    newDf = appendZeroes(tobecleaned_df, column_name, lengthOfField)
  }
  return newDf
}

//This function will find unmatched columns between source ahnd target dataframes. 
//It takes source dataframe, target dataframe, source primary, target primary key
//returns a list of Dataframes which has srcKeyColumnName, srcMismatchColumnName, tarKeyColumnName, tarMismatchColumnName
def findUnmatchedColumns_5rows(src_df: DataFrame, tar_df: DataFrame, srcpk: String, tarpk: String): List[DataFrame] = {
  var unmatchedDf: DataFrame = null
  var unmatchedDf_temp: DataFrame = Seq.empty[(String, String, String, String, String)].toDF("primary_key", "source_column_name", "target_column_name", "source_value", "target_value")
  val src_columns: List[String] = src_df.schema.fields.map(_.name).toList
  val tar_columns: List[String] = tar_df.schema.fields.map(_.name).toList
  
  var mergedColumns: List[String] = src_columns zip tar_columns map { case (a, b) => a + "," + b }
  val selectiveDifferences_df: List[DataFrame] = mergedColumns.map(mergedColumn => {// List[List[Dataset[Row]]]
    var col_arry = mergedColumn.split(",")
    val startTimeMillis_1: Long = System.currentTimeMillis()    
    var src_rows:Dataset[Row] = src_df.select(srcpk, col_arry(0)).except(tar_df.select(tarpk, col_arry(1)))
    var tar_rows:Dataset[Row] = tar_df.select(tarpk, col_arry(1)).except(src_df.select(srcpk, col_arry(0)))
    val startTimeMillis_2: Long = System.currentTimeMillis()
    var unmatchedCol_df:DataFrame = spark.emptyDataFrame
    
    if(src_rows.count > 0 || tar_rows.count > 0){
      //This block is for getting columns which have unmatched data
      var src_unmatchedrows_df: DataFrame = src_rows.toDF()
      var tar_unmatchedrows_df: DataFrame = tar_rows.toDF()
           
      src_unmatchedrows_df.createOrReplaceTempView("src_unmatchedrows_df") 
      tar_unmatchedrows_df.createOrReplaceTempView("tar_unmatchedrows_df") 
      
      unmatchedCol_df = spark.sql("select src_unmatchedrows_df.*,tar_unmatchedrows_df.* from src_unmatchedrows_df, tar_unmatchedrows_df where src_unmatchedrows_df." + srcpk + " = tar_unmatchedrows_df." + tarpk)
      unmatchedCol_df.createOrReplaceTempView("unmatchedCol_df")       
             
    }
    unmatchedCol_df    
  })  
    selectiveDifferences_df
}

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
    var isAutoAnalysisRequired: Boolean = true
    //val strSourceName = "avaya"
    //val strRunDate = "2019-06-26"
    
    // CONNECTING TO THE LANDING ZONE
	spark.conf.set("fs.azure.account.key.edplztstsa00.blob.core.windows.net", dbutils.secrets.get(scope = "adbtstkv00", key = "edplztstsa00key"))


	// PATH TO THE TESTDATA CSV HAVING ALL TEST DATA	
    var parenttablelistpath = "wasbs://test-parameters@edplztstsa00.blob.core.windows.net/table_list/csv_pqt/CSV_PQT_TestData_WIP.csv"
	
	// *****LOADING TABLE LIST*****
	var parentTable_list = spark.read.format("csv").option("header", "true").load(parenttablelistpath).toDF
	parentTable_list.createOrReplaceTempView("parentTable_list")
	spark.table("parentTable_list").cache    

	var pr_lst_DataSource = parentTable_list.select("system").collect().map(_(0)).toList
	var pr_lst_tables = parentTable_list.select("tablename").collect().map(_(0)).toList
    var pr_lst_csv_pks:List[String] = parentTable_list.select("csv_pk_column").collect().map(_.getString(0)).toList
    var pr_lst_pqt_pks:List[String] = parentTable_list.select("pqt_pk_column").collect().map(_.getString(0)).toList
	var pr_lst_RunDate = parentTable_list.select("rundate").collect().map(_(0)).toList	
	var pr_lst_DataClassification = parentTable_list.select("dataclassification").collect().map(_(0)).toList

    // *****LOADING KNOWN ISSUES LIST*****
    var knownissueslistpath = "wasbs://test-parameters@edplztstsa00.blob.core.windows.net/table_list/csv_pqt/known_data_issues_WIP.csv"
    var knownissues_list = spark.read.format("csv").option("header", "true").load(knownissueslistpath).toDF
    knownissues_list.createOrReplaceTempView("knownissues_list")

    // **************************************************************************************************************************************************
    // Start of the Loop
    // **************************************************************************************************************************************************
    println("********** Execute data testing for CSV and Parquet files **********\n")
   
    for (iterator <- 0 until pr_lst_tables.length) 
    {
      var srcTableName = pr_lst_tables(iterator)
      
      //println("inside loop - 1")
      var strJsonFilePath = ""
      
      if(pr_lst_DataSource(iterator).equals("avaya")){
        strJsonFilePath = "wasbs://edp-etl-pipeline-inputs@" + strUsername + ".blob.core.windows.net/meta/typing/" + pr_lst_DataSource(iterator) + "/" + "inputTableName" + "/" + pr_lst_DataSource(iterator) + "_" + "inputTableName" + "_meta.json"
      }else if((pr_lst_DataSource(iterator).equals("isu"))){
        strJsonFilePath = "wasbs://edp-etl-pipeline-inputs@" + strUsername + ".blob.core.windows.net/meta/typing/" + pr_lst_DataSource(iterator) + "/" + "inputTableName" + "/1/" + pr_lst_DataSource(iterator) + "_" + "inputTableName" + "_meta_1.json"        
      }else if((pr_lst_DataSource(iterator).equals("crm"))){
        strJsonFilePath = "wasbs://edp-etl-pipeline-inputs@" + strUsername + ".blob.core.windows.net/meta/typing/" + pr_lst_DataSource(iterator) + "/" + "inputTableName" + "/1/" + pr_lst_DataSource(iterator) + "_" + "inputTableName" + "_meta_1.json"        
      }     
      

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

        var strJsonFilePathnInner = strJsonFilePath.replaceAll("inputTableName", s"${srcTableName}")
        
        try {
          var df_jsonfile_loop = spark.read
            .option("parserLib", "univocity")
            .option("multiLine", "true")
            .json(strJsonFilePathnInner)
          var df_JsonTemp_stg = df_jsonfile_loop.select(selcolumnNameJason.map(name => col(name)): _*).filter("name !='di_sequence_number'").filter("name !='di_operation_type'").filter("name !='odq_changemode'").filter("name !='odq_entitycntr'").filter("name !='ds_update_ts'")
          var df_JsonTemp = df_JsonTemp_stg.withColumnRenamed("name", "fieldname").withColumnRenamed("type", "targettype")//.withColumnRenamed("length", "length")
          df_JsonTemp.createOrReplaceTempView("df_JsonTemp_" + srcTableName)     

          strsql = strsql.concat(" select * from df_JsonTemp_" + srcTableName + " union all ")
        } catch {
          case e: Exception => println(e.getMessage())
        }

      strsqlUnion = strsql.substring(0, strsql.length - 10)

      var df_json = spark.sql(strsqlUnion)
      spark.catalog.dropTempView("df_JsonTemp_" + srcTableName)

      //df_json.show()
      //spark.catalog.dropTempView("df_JsonTemp_" + srcTableName)

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
      var strParquetFilePathInner = strParquetFilePath.replaceAll("inputTableName", s"${srcTableName}")
      try {

        var df_Parquet_Temp = spark.read.parquet(strParquetFilePathInner)

        df_Parquet_Temp.createOrReplaceTempView("df_Parque_Temp_" + srcTableName)
        df_Parquet_metadata_stg = spark.sql("DESCRIBE table df_Parque_Temp_" + srcTableName)
        df_Parquet_metadata_stg.createOrReplaceTempView("df_Parquet_Temp_meta")
        //println("Parquet meta data:")
        //df_Parquet_metadata_stg.show()

        //df_Parquet_data = df_Parquet_data.union(spark.sql("select '" + srcTableName + "' as tablename, a.* from df_Parquet_Temp_meta a"))
        df_Parquet_data = df_Parquet_data.union(spark.sql("select col_name,data_type from df_Parquet_Temp_meta"))
        
        //df_Parquet_data.show()
        
      } catch {
        case e: Exception => println(e.getMessage())
      }

      spark.catalog.dropTempView("df_Parque_Temp_" + srcTableName)
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
        
        df_di_parquet = df_di_parquet.union(spark.sql("select '" + srcTableName + "' as tablename,count(*) as mis_natch_count, case when count(*) != 0 then 'FAIL' else 'PASS' end as data_value_status from (select fieldname,case when targettype='integer' then 'int' when targettype='time' then 'string' else targettype end as targettype from dd03l_json_data minus select columnname, case when datatype like 'decimal%' then 'decimal' else datatype end as datatype from df_Parquet_data where columnname not in ('_filename','_index','_errors','_ts') )"))
       
       println("Testing for table : " + srcTableName)
       println(" ")
        
       
       println("Metajson file metadata value:")
       dd03l_json_data.show(dd03l_json_data.count.toInt, false)
       println("Parquet meta data value:")
       df_Parquet_data.show(df_Parquet_data.count.toInt, false)
       println("Execute mete data value testing (Json - Parquet )")
       df_di_parquet.show(df_di_parquet.count.toInt, false)
       df_di_parquet.createOrReplaceTempView("df_di_parquet")

       // **************************************************************************************************************************************************
       // CSV and Parqut Count Comparison
       // **************************************************************************************************************************************************
        
        var csvPath = ""
        if(pr_lst_DataSource(iterator).equals("avaya")){
          csvPath = "wasbs://" + "others@" + strUsername + ".blob.core.windows.net/" + pr_lst_DataSource(iterator) + "/" + srcTableName + "/1/extract_date=" + pr_lst_RunDate(iterator) + "/*.csv.bz2"
        }else if((pr_lst_DataSource(iterator).equals("isu"))){
          csvPath = "wasbs://" + pr_lst_DataSource(iterator) + "@" + strUsername + ".blob.core.windows.net/" + srcTableName + "/1/extract_date=" + pr_lst_RunDate(iterator) + "/*.csv.bz2"
        }else if((pr_lst_DataSource(iterator).equals("crm"))){
          csvPath = "wasbs://" + pr_lst_DataSource(iterator) + "@" + strUsername + ".blob.core.windows.net/" + srcTableName + "/1/extract_date=" + pr_lst_RunDate(iterator) + "/*.csv.bz2"
        }
        
        var df_csv = spark.read.format("csv").option("header", "false").option("delimiter", "\u0001").load(csvPath)
        //df_csv = df_csv.drop("_c2","_c3")       
       
        df_csv.createOrReplaceTempView("csv")

        val csvCount = df_csv.count()

        var df_pqt = spark.read.parquet(strParquetFilePathInner) 
        df_pqt = df_pqt.drop("monotonically_increasing_id","_monotonically_increasing_id","_filename","filename")        
        //df_pqt = df_pqt.drop("ds_update_ts")        

        df_pqt.createOrReplaceTempView("pqt")

        val pqtCount = df_pqt.count()

        //pqtCount = sqlContext.sql("SELECT count(*) FROM pqt")

        println("CSV Count for " + srcTableName + " is : " + csvCount + " records ")

        println("PQT Count for " + srcTableName + " is : " + pqtCount + " records ")
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
        
        //Below code will apply transformation on the dataframes as per known issues
        //var kwnissues_orgn_colnme_func_df = knownissues_list.select("table_name","dataorigin","column_name","function","issuearea").filter("functionality='avaya_typingloading'").filter("table_name='" + srcTableName + "'")
        var kwnissues_orgn_colnme_func_df = knownissues_list.select("table_name","dataorigin","column_name","function","issuearea").filter("table_name='" + srcTableName + "'")
        if(!kwnissues_orgn_colnme_func_df.limit(1).collect().isEmpty){
          var csv_kwnissues_orgn_colnme_func_df = kwnissues_orgn_colnme_func_df.filter("dataorigin='csv'")
          var pqt_kwnissues_orgn_colnme_func_df = kwnissues_orgn_colnme_func_df.filter("dataorigin='pqt'")
          csv_kwnissues_orgn_colnme_func_df.createOrReplaceTempView("csv_kwnissues_orgn_colnme_func_df")  
          pqt_kwnissues_orgn_colnme_func_df.createOrReplaceTempView("pqt_kwnissues_orgn_colnme_func_df")        
          //Clean dataframe as per known issues      
          var df_csv_temp: DataFrame = fixKnownDataIssues(df_csv,csv_kwnissues_orgn_colnme_func_df)
          var df_pqt_temp: DataFrame = fixKnownDataIssues(df_pqt,pqt_kwnissues_orgn_colnme_func_df)
          df_csv_temp.createOrReplaceTempView("df_csv_temp")
          df_pqt_temp.createOrReplaceTempView("df_pqt_temp")
          df_csv = df_csv_temp
          df_pqt = df_pqt_temp        
          println("Following transformations were applied on the CSV dataframe to handle known issues: ")
          csv_kwnissues_orgn_colnme_func_df.show(csv_kwnissues_orgn_colnme_func_df.count().toInt, false)
          println("Following transformations were applied on the PQT dataframe to handle known issues: ")
          pqt_kwnissues_orgn_colnme_func_df.show(pqt_kwnissues_orgn_colnme_func_df.count().toInt, false)
        }
        
        var df_tempCsv = df_csv   
        val df_tempPqt = df_pqt

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
        spark.sql("SELECT * FROM ResultCsv" ).show(5,false)
        println("Data mismatch count in between CSV and PQT :" + csvDataCount + " records ")

        println(" ")

        println("PQT Vs CSV comparison output:")
        spark.sql("SELECT * FROM ResultPqt" ).show(5,false)
        println("Data mismatch count in between PQT and CSV :" + pqtDataCount + " records ")
        
        if((csvDataCount != 0) || (pqtDataCount != 0)){
            println("CSV data not matched with PQT data - FAIL")
            println("Top 10 Sequence number not matching: ")            
            var csvpk: String = pr_lst_csv_pks(iterator)//"_c50"
            var pqtpk: String = pr_lst_pqt_pks(iterator)//"seq_no"
            df_resultCsv.select(csvpk).limit(10).show(5,false)
            df_resultPqt.select(pqtpk).limit(10).show(5,false)
            if(isAutoAnalysisRequired){
              println("Below is the dataframe of columns not matching: ")
              var unmatched_cols_dfs:List[DataFrame] = findUnmatchedColumns_5rows(df_resultCsv, df_resultPqt, csvpk, pqtpk)
              unmatched_cols_dfs.filter(unmatched_cols_df => unmatched_cols_df.count() > 0).foreach(unmatched_cols_df => unmatched_cols_df.show)
            }
          
        }else{
             println("CSV data matched with PQT data successfully - PASS")
         }

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