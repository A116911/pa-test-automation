//Compare full extract CSV with DB = no mismatch
//libraries
import scala.util.control._
import java.util.Properties
import java.text.SimpleDateFormat
import java.util.Calendar
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col,lower,when,length,concat,lit,trim,unix_timestamp}
import org.apache.spark.sql.{DataFrame,Column,Dataset,Row}
import org.apache.spark.sql.types.{DataTypes,TimestampType}
import scala.collection.immutable.List
import java.time.LocalDateTime;

val startTimeMillis = System.currentTimeMillis()

// Check whether column exists in dataframe
def hasColumn(df: DataFrame, colName: String): Boolean = {
  df.columns.contains(colName)
}

//This function will convert string timestamp in csv file from yyyy-MM-dd'T'HH:mm:ss.SSS'Z' to yyyy-MM-dd'T'HH:mm:ss.SSS+0000
def csv_yyyy_mm_dd_t_hh_mm_ss_sssz_to_yy_mm_dd_t_hh_mm_ss_sss(df: DataFrame, colName: String): DataFrame = {  
  var newDf = df.withColumn(colName, unix_timestamp(col(colName), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").cast(TimestampType))
  return newDf
}

//This function will convert string timestamp in SQL database from yyyy-MM-dd'T'HH:mm:ss.SSS+0000 to yyyy-MM-dd'T'HH:mm:ss.SSS+0000
def db_yyyy_mm_dd_t_hh_mm_ss_sssz_to_yy_mm_dd_t_hh_mm_ss_sss(df: DataFrame, colName: String): DataFrame = {
  var newDf = df.withColumn(colName, unix_timestamp(col(colName), "yyyy-MM-dd'T'HH:mm:ss.SSS'+0000'").cast(TimestampType))
  return newDf
}

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
  
  if(hasColumn(tobecleaned_df, column_name)){
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
    }else if(func_name.toLowerCase().trim().equals("csv_yyyy_mm_dd_t_hh_mm_ss_sssz_to_yy_mm_dd_t_hh_mm_ss_sss")){
      newDf = csv_yyyy_mm_dd_t_hh_mm_ss_sssz_to_yy_mm_dd_t_hh_mm_ss_sss(tobecleaned_df, column_name)
    }else if(func_name.toLowerCase().trim().equals("db_yyyy_mm_dd_t_hh_mm_ss_sssz_to_yy_mm_dd_t_hh_mm_ss_sss")){
      newDf = db_yyyy_mm_dd_t_hh_mm_ss_sssz_to_yy_mm_dd_t_hh_mm_ss_sss(tobecleaned_df, column_name)
    }else{
      newDf = tobecleaned_df
      println("Skipping function. Invalid function name: " + func_name)      
    }
  }else{
    newDf = tobecleaned_df
    println("Column does not exist in Dataframe: " + column_name)
    println("Skipping transformation: " + func_name)
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
    unmatchedCol_df.show(5,false)
    unmatchedCol_df    
  })  
    selectiveDifferences_df
}

 
//DB Connection String
val jdbcHostname = "10.216.2.181"
val jdbcPort = 1433
val connectionProperties = new Properties()
val driverClass = "com.microsoft.sqlserver.jdbc.SQLServerDriver"


connectionProperties.put("user", s"CDLread")
connectionProperties.put("password", dbutils.secrets.get(scope = "adbtstkv00", key = "avayadb"))    
connectionProperties.setProperty("Driver", driverClass)      

var isAutoAnalysisRequired: Boolean = false
// CONNECTING TO THE LANDING ZONE
dbutils.fs.refreshMounts()
spark.conf.set("fs.azure.account.key.edplztstsa00.blob.core.windows.net", dbutils.secrets.get(scope = "adbtstkv00", key = "edplztstsa00key"))
val strUsername ="edplztstsa00"       // Source blob location(CSV and JSON)
val lz_blobName = "others"
val sourcefolder = "avaya"

// *****LOADING TABLE LIST*****
var parenttablelistpath = "wasbs://test-parameters@edplztstsa00.blob.core.windows.net/table_list/db_csv/ingestion_avaya/DB_CSV_TestData_Day0_Delta.csv"
var parentTable_list = spark.read.format("csv").option("header", "true").load(parenttablelistpath).toDF
parentTable_list.createOrReplaceTempView("parentTable_list")
var tableNames = parentTable_list.select("tablename").filter("sourcefolder='"+ sourcefolder +"'").collect().map(_(0)).toList
var day0_extractDates = parentTable_list.select("day0_extract_date").filter("sourcefolder='"+ sourcefolder +"'").collect().map(_(0)).toList
var day1_extractDates = parentTable_list.select("day1_extract_date").filter("sourcefolder='"+ sourcefolder +"'").collect().map(_(0)).toList
var pr_lst_db_pks:List[String] = parentTable_list.select("db_pk_column").collect().map(_.getString(0)).toList
var pr_lst_csv_pks:List[String] = parentTable_list.select("csv_pk_column").collect().map(_.getString(0)).toList
var is_validation_reqd_lst:List[String] = parentTable_list.select("runValidation").collect().map(_.getString(0)).toList



// *****LOADING KNOWN ISSUES LIST*****
var knownissueslistpath = "wasbs://test-parameters@edplztstsa00.blob.core.windows.net/table_list/db_csv/ingestion_avaya/known_data_issues.csv"
var knownissues_list = spark.read.format("csv").option("header", "true").load(knownissueslistpath).toDF
knownissues_list.createOrReplaceTempView("knownissues_list")

for (iterator <- 0 until tableNames.length){
     var startTimeForTableMillis = System.currentTimeMillis()
     var is_validation_reqd = is_validation_reqd_lst(iterator)    
     var srcTableName = tableNames(iterator)
    if(is_validation_reqd.equals("yes")){      
    
     var day0_extractDate = day0_extractDates(iterator)
     var day1_extractDate = day1_extractDates(iterator)
     var jdbcDatabase = ""//"Users"//"CentralDWH_LS"
     
     if(srcTableName.equals("acs_results_withfcr_ls")){
       jdbcDatabase = "Users"
     }else{
       jdbcDatabase = "CentralDWH_LS"
     }
     val jdbcUrl = s"jdbc:sqlserver://10.216.2.181:1433;database=" + jdbcDatabase + ";"
  
    // Read Avaya data into dataframe    
     var df_srcTable = spark.read.jdbc(jdbcUrl, "dbo." + srcTableName, connectionProperties)
     df_srcTable.createOrReplaceTempView("df_srcTable")

    //Read CSV into dataframe
     var blob_folderName = "wasbs://" + lz_blobName + "@" + strUsername + ".blob.core.windows.net/" + sourcefolder + "/" + srcTableName     
     var day1_delta_csv_path = blob_folderName + "/1/extract_date=" + day1_extractDate
     var day1_temp_full_csv_path = blob_folderName + "/temp/full/extract_date=" + day1_extractDate
     var day0_temp_full_csv_path = blob_folderName + "/temp/full/extract_date=" + day0_extractDate  

     var day1_delta_csv_filename = dbutils.fs.ls(day1_delta_csv_path)
     var day1_temp_full_csv_filename = dbutils.fs.ls(day1_temp_full_csv_path)
     var day0_temp_full_csv_filename = dbutils.fs.ls(day0_temp_full_csv_path)
            
     println("\n")
     println("******************START TESTCASE EXECUTION*************************")
     println("Database table name: " + srcTableName)  
     println("Day 0 extract Date: " + day0_extractDate)     
     println("Day 0 Temp CSV File names: ")      
     day0_temp_full_csv_filename.map(_.path).filter(_.contains("csv.bz2")).foreach(path => println("" + path))
     
     println("Day 1 extract Date: " + day1_extractDate)     
     println("Day 1 Temp CSV File names: ")      
     day1_temp_full_csv_filename.map(_.path).filter(_.contains("csv.bz2")).foreach(path => println("" + path))     
     
     println("Day 1 Delta CSV File names: ")      
     day1_delta_csv_filename.map(_.path).filter(_.contains("csv.bz2")).foreach(path => println("" + path))
  
  
     println("Test case execution date: " + LocalDateTime.now())     
     println("\n")
     
     var df_day1_delta_csv = spark.read.format("csv").option("header", "false").option("delimiter", "\u0001").load(day1_delta_csv_path + "/*.csv.bz2")
     var df_day1_temp_full_csv = spark.read.format("csv").option("header", "true").option("delimiter", "\u0001").load(day1_temp_full_csv_path + "/*.csv.bz2")
     var df_day0_temp_full_csv = spark.read.format("csv").option("header", "true").option("delimiter", "\u0001").load(day0_temp_full_csv_path + "/*.csv.bz2")  
     df_day1_delta_csv.createOrReplaceTempView("df_day1_delta_csv")
     df_day1_temp_full_csv.createOrReplaceTempView("df_day1_temp_full_csv")
     df_day0_temp_full_csv.createOrReplaceTempView("df_day0_temp_full_csv")
    
      // **************************************************************************************************************************************************
      // Code to validate data
      // **************************************************************************************************************************************************
    
     //Below code will apply transformation on the dataframes as per known issues
     var kwnissues_orgn_colnme_func_df = knownissues_list.select("table_name","dataorigin","column_name","function","issuearea").filter("functionality='avaya_ingestion'").filter("table_name='" + srcTableName + "'")
     if(!kwnissues_orgn_colnme_func_df.limit(1).collect().isEmpty){
       var db_kwnissues_orgn_colnme_func_df = kwnissues_orgn_colnme_func_df.filter("dataorigin='database'")
       var csv_kwnissues_orgn_colnme_func_df = kwnissues_orgn_colnme_func_df.filter("dataorigin='csv'")  
       var tempcsv_kwnissues_orgn_colnme_func_df = kwnissues_orgn_colnme_func_df.filter("dataorigin='temp_csv'")
       db_kwnissues_orgn_colnme_func_df.createOrReplaceTempView("db_kwnissues_orgn_colnme_func_df")
       csv_kwnissues_orgn_colnme_func_df.createOrReplaceTempView("csv_kwnissues_orgn_colnme_func_df")
       tempcsv_kwnissues_orgn_colnme_func_df.createOrReplaceTempView("tempcsv_kwnissues_orgn_colnme_func_df")
       //Clean dataframe as per known issues
       var df_srcTable_temp: DataFrame = fixKnownDataIssues(df_srcTable,db_kwnissues_orgn_colnme_func_df)
       var df_day1_delta_csv_temp: DataFrame = fixKnownDataIssues(df_day1_delta_csv,csv_kwnissues_orgn_colnme_func_df)
       var df_day1_temp_full_csv_temp: DataFrame = fixKnownDataIssues(df_day1_temp_full_csv,tempcsv_kwnissues_orgn_colnme_func_df) 
       var df_day0_temp_full_csv_temp: DataFrame = fixKnownDataIssues(df_day0_temp_full_csv,tempcsv_kwnissues_orgn_colnme_func_df)        
       
       df_srcTable_temp.createOrReplaceTempView("df_srcTable_temp")
       df_day1_delta_csv_temp.createOrReplaceTempView("df_day1_delta_csv_temp")
       df_day1_temp_full_csv_temp.createOrReplaceTempView("df_day1_temp_full_csv_temp")
       df_day0_temp_full_csv_temp.createOrReplaceTempView("df_day0_temp_full_csv_temp")
        
       df_srcTable = df_srcTable_temp   
       df_day1_delta_csv = df_day1_delta_csv_temp  
       df_day1_temp_full_csv = df_day1_temp_full_csv_temp   
       df_day0_temp_full_csv = df_day0_temp_full_csv_temp
       println("Following transformations were applied on the DB table dataframe to handle known issues: ")
       db_kwnissues_orgn_colnme_func_df.show(db_kwnissues_orgn_colnme_func_df.count().toInt, false)    
       println("Following transformations were applied on the Delta CSV dataframe to handle known issues: ")
       csv_kwnissues_orgn_colnme_func_df.show(csv_kwnissues_orgn_colnme_func_df.count().toInt, false)
       println("Following transformations were applied on the Temp full CSV dataframe to handle known issues: ")
       tempcsv_kwnissues_orgn_colnme_func_df.show(tempcsv_kwnissues_orgn_colnme_func_df.count().toInt, false)
     }
      
     var count_day1_delta_csv = df_day1_delta_csv.count()
     var count_day1_temp_full_csv = df_day1_temp_full_csv.count()
     println("Count of records in Day 1 delta CSV file: " + count_day1_delta_csv)
     println("Count of records in Day 1 temp full CSV file: " + count_day1_temp_full_csv)
     println("Count of records in Day 1 DB: " + df_srcTable.count())
     println("Count of records in Day 0 temp full CSV file: " + df_day0_temp_full_csv.count())
     println("\n\n")
      
      //Code to validate data between Day 1 temp csv file and Day 1 DB
     println("Execute comparison of Day 1 temp csv file vs Day 1 DB: ")
     var df_result_day1tempfullcsv_minus_day1db = df_day1_temp_full_csv.except(df_srcTable)
     var count_result_day1tempfullcsv_minus_day1db = df_result_day1tempfullcsv_minus_day1db.count()
     df_result_day1tempfullcsv_minus_day1db.createOrReplaceTempView("df_result_day1tempfullcsv_minus_day1db")  
     println("Count of mismatched records between Day 1 temp csv file except Day 1 DB: " + count_result_day1tempfullcsv_minus_day1db)
     if(count_result_day1tempfullcsv_minus_day1db == 0){
        println("Validate no data mismatch between Day 1 temp csv file and Day 1 DB - PASS")
     }else{
        println("Validate no data mismatch between Day 1 temp csv file and Day 1 DB - FAIL")
     }  
     println("\n\n")
  
    //Code to validate data between Day 1 temp full csv file and Day 0 temp full CSV
    //Store the differences in DF
    //Compare the differences dataframe with day1 delta CSV
     println("Execute comparison of Day 1 temp full csv vs Day 0 temp full csv vs Day 1 delta csv: ") 
     var df_result_day1tempfullcsv_minus_day0tempfullcsv = df_day1_temp_full_csv.except(df_day0_temp_full_csv)
     var count_result_day1tempfullcsv_minus_day0tempfullcsv = df_result_day1tempfullcsv_minus_day0tempfullcsv.count()
     println("Count of mismatched records between Day 1 temp csv file except Day 0 temp csv file: " + count_result_day1tempfullcsv_minus_day0tempfullcsv)
     if(count_day1_delta_csv != 0){
       var df_diff_minus_day1deltacsv = df_result_day1tempfullcsv_minus_day0tempfullcsv.except(df_day1_delta_csv)
       var df_day1deltacsv_minus_diff = df_day1_delta_csv.except(df_result_day1tempfullcsv_minus_day0tempfullcsv)
       var count_diff_minus_day1deltacsv = df_diff_minus_day1deltacsv.count()
       var count_day1deltacsv_minus_diff = df_day1deltacsv_minus_diff.count()
       println("Count of mismatched records between differences of Day1 and Day0 temp file except Day 1 delta csv file: " + count_diff_minus_day1deltacsv)
       println("Count of mismatched records between Day 1 delta csv file except differences of Day1 and Day0 temp file: " + count_day1deltacsv_minus_diff)
       if((count_diff_minus_day1deltacsv == 0) && (count_day1deltacsv_minus_diff == 0)){
        println("Validate delta data between Day 1 temp full csv file except Day 0 temp full CSV should match with Day1 delta csv file - PASS")
       }else{
        println("Validate delta data between Day 1 temp full csv file except Day 0 temp full CSV should match with Day1 delta csv file - FAIL")
       }
      }else{
           println("0 records in Delta csv file. Hence skipping comparison of Day 1 temp full csv vs Day 0 temp full csv vs Day 1 delta csv")
      }
     println("\n\n")
      
  
    //Code to validate data between Day 1 delta file and Day 1 DB
     println("Execute comparison of Day 1 delta csv vs Day 1 DB: ") 
     if(count_day1_delta_csv != 0){
       var df_result_day1delta_minus_day1db = df_day1_delta_csv.except(df_srcTable)
       var count_result_day1delta_minus_day1db = df_result_day1delta_minus_day1db.count()
       df_result_day1delta_minus_day1db.createOrReplaceTempView("df_result_day1delta_minus_day1db")  
       println("Count of mismatched records between Day 1 delta file except Day 1 DB: " + count_result_day1delta_minus_day1db)
       if(count_result_day1delta_minus_day1db == 0){
          println("Validate no data mismatch between Day 1 delta file and Day 1 DB - PASS")
       }else{
          println("Validate no data mismatch between Day 1 delta file and Day 1 DB - FAIL")
       }
     }else{
       println("0 records in Delta csv file. Hence skipping comparison with Day 1 delta csv vs Day 1 DB")
     }     
     println("\n\n")
      
    //Code to validate data between Day 1 delta file and Day 1 temp full CSV
     println("Execute comparison of Day 1 delta csv vs Day 1 temp full csv: ")
     if(count_day1_delta_csv != 0){
       var df_result_day1deltacsv_minus_day1tempfullcsv = df_day1_delta_csv.except(df_day1_temp_full_csv)
       var df_result_day1tempfullcsv_minus_day1delta = df_day1_temp_full_csv.except(df_day1_delta_csv)
       var count_result_day1deltacsv_minus_day1tempfullcsv = df_result_day1deltacsv_minus_day1tempfullcsv.count()
       var count_result_day1tempfullcsv_minus_day1deltacsv = df_result_day1tempfullcsv_minus_day1delta.count()
       df_result_day1deltacsv_minus_day1tempfullcsv.createOrReplaceTempView("df_result_day1deltacsv_minus_day1tempfullcsv")  
       df_result_day1tempfullcsv_minus_day1delta.createOrReplaceTempView("df_result_day1tempfullcsv_minus_day1delta")  
       println("Count of mismatched records between Day 1 delta file except Day 1 temp full CSV: " + count_result_day1deltacsv_minus_day1tempfullcsv) 
       println("Count of mismatched records between Day 1 temp full CSV except Day 1 delta csv: " + count_result_day1tempfullcsv_minus_day1deltacsv)
       var count_day1tempcsv_minus_day1deltacsv = count_day1_temp_full_csv - count_day1_delta_csv
       println("Substraction of records in Day 1 temp full CSV and Day 1 delta csv: " + count_day1tempcsv_minus_day1deltacsv)
       if((count_result_day1deltacsv_minus_day1tempfullcsv == 0) && (count_day1tempcsv_minus_day1deltacsv == count_result_day1tempfullcsv_minus_day1deltacsv)){
          println("Validate no data mismatch between Day 1 delta file and Day 1 temp csv file - PASS")       
       }else{
          println("Validate no data mismatch between Day 1 delta file and Day 1 temp csv file - FAIL")
       }
     }else{
       println("0 records in Delta csv file. Hence skipping comparison Day 1 delta csv vs Day 1 temp full csv")
     }
     println("\n\n") 
  
     
     var endTimeForTableMillis = System.currentTimeMillis()    
     println("Total time in minutes for current test case is: " + ((endTimeForTableMillis-startTimeForTableMillis)/(1000 * 60)).toFloat)
     println("\n******************END TESTCASE EXECUTION*************************\n")
}else{
      println("Skipping validation. runValidation flag set to NO in testdata.csv for table: " + srcTableName)
    }
}
val endTimeMillis = System.currentTimeMillis()
println("Total time in minutes for entire test suite: " + ((endTimeMillis-startTimeMillis)/(1000 * 60)).toFloat)
println("\n#################################################################################\n")  
println("Note: Refer GIT for automation code used in above testing.")
