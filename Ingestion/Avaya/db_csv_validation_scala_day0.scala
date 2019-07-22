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
//val jdbcDatabase = "CentralDWH_LS"  
//val jdbcUrl = s"jdbc:sqlserver://10.216.2.181:1433;database=CentralDWH_LS;"
//val jdbcUrl = s"jdbc:sqlserver://10.216.2.181:1433;database=" + jdbcDatabase + ";"
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
var tableNames:List[String] = parentTable_list.select("tablename").filter("sourcefolder='"+ sourcefolder +"'").collect().map(_.getString(0)).toList
//var extractDates = parentTable_list.select("extract_date").filter("sourcefolder='"+ sourcefolder +"'").collect().map(_(0)).toList
var day0_extractDates = parentTable_list.select("day0_extract_date").filter("sourcefolder='"+ sourcefolder +"'").collect().map(_(0)).toList
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
     var srcTableName: String = tableNames(iterator)
     var day0_extractDate = day0_extractDates(iterator)
     if(is_validation_reqd.equals("yes")){       
          
     var jdbcDatabase = ""//"Users"//"CentralDWH_LS"
     
     if(srcTableName.equals("acs_results_withfcr_ls")){
       jdbcDatabase = "Users"
     }else{
       jdbcDatabase = "CentralDWH_LS"
     }
     var jdbcUrl = s"jdbc:sqlserver://10.216.2.181:1433;database=" + jdbcDatabase + ";"
  
     
     var extractDate = day0_extractDate     
     
  
    // Read Avaya data into dataframe    
     var df_srcTable = spark.read.jdbc(jdbcUrl, "dbo." + srcTableName, connectionProperties)
     df_srcTable.createOrReplaceTempView("df_srcTable")

    //Read CSV into dataframe
     var blob_folderName = "wasbs://" + lz_blobName + "@" + strUsername + ".blob.core.windows.net/" + sourcefolder + "/" + srcTableName
     
     var blob_folderName_extractPath = blob_folderName + "/1/extract_date=" + extractDate
     var csvFileNames = dbutils.fs.ls(blob_folderName_extractPath)     
            
     println("\n")
     println("******************START TESTCASE EXECUTION*************************")
     println("Database table name: " + srcTableName)
     println("Extract Date: " + extractDate)
     println("CSV File names: ")      
     csvFileNames.map(_.path).filter(_.contains("csv.bz2")).foreach(path => println("" + path))
     println("Test case execution date: " + LocalDateTime.now())     
     println("\n")
     
     var df_csv = spark.read.format("csv").option("header", "false").option("delimiter", "\u0001").load(blob_folderName_extractPath + "/*.csv.bz2")
     df_csv.createOrReplaceTempView("df_csv")
  
     //Code to validate counts between DB table and CSV
     println("Execute count comparison between Database table and CSV") 
     var srcTable_count = df_srcTable.count() 
     var csv_count = df_csv.count()
     println("Database table Count is : " + srcTable_count)
     println("CSV Count is : " + csv_count)
     

     if ((srcTable_count - csv_count) == 0){
         println("Data count matching for both Database table and CSV --- PASS.")
     } else{
         println("Data count not matching for both Database table and CSV --- FAIL.")
     }
     println("*******************************************")
     println("\n")
     
     println("Execute data comparison between Database table and CSV ")
    
  
      // **************************************************************************************************************************************************
      // Code to validate data
      // **************************************************************************************************************************************************
    
     //Below code will apply transformation on the dataframes as per known issues
     var kwnissues_orgn_colnme_func_df = knownissues_list.select("table_name","dataorigin","column_name","function","issuearea").filter("functionality='avaya_ingestion'").filter("table_name='" + srcTableName + "'")
     if(!kwnissues_orgn_colnme_func_df.limit(1).collect().isEmpty){
       var db_kwnissues_orgn_colnme_func_df = kwnissues_orgn_colnme_func_df.filter("dataorigin='database'")
       var csv_kwnissues_orgn_colnme_func_df = kwnissues_orgn_colnme_func_df.filter("dataorigin='csv'")  
       db_kwnissues_orgn_colnme_func_df.createOrReplaceTempView("db_kwnissues_orgn_colnme_func_df")
       csv_kwnissues_orgn_colnme_func_df.createOrReplaceTempView("csv_kwnissues_orgn_colnme_func_df")
       //Clean dataframe as per known issues      
        var df_srcTable_temp: DataFrame = fixKnownDataIssues(df_srcTable,db_kwnissues_orgn_colnme_func_df)
        var df_csv_temp: DataFrame = fixKnownDataIssues(df_csv,csv_kwnissues_orgn_colnme_func_df)        
        df_srcTable_temp.createOrReplaceTempView("df_srcTable_temp")
        df_csv_temp.createOrReplaceTempView("df_csv_temp")     
        df_srcTable = df_srcTable_temp   
        df_csv = df_csv_temp     
        println("Following transformations were applied on the DB table dataframe to handle known issues: ")
        db_kwnissues_orgn_colnme_func_df.show(db_kwnissues_orgn_colnme_func_df.count().toInt, false)    
        println("Following transformations were applied on the CSV dataframe to handle known issues: ")
        csv_kwnissues_orgn_colnme_func_df.show(csv_kwnissues_orgn_colnme_func_df.count().toInt, false)
     }
     
     println("Dataframe comparison between source and target")
  
     var df_resultSrcTableMinCsv = df_srcTable.except(df_csv) 
     var df_resultCsvMinSrcTable = df_csv.except(df_srcTable)    
     df_resultSrcTableMinCsv.createOrReplaceTempView("df_resultSrcTableMinCsv")
     df_resultCsvMinSrcTable.createOrReplaceTempView("df_resultCsvMinSrcTable")
     var srcTable_minus_csv_count = df_resultSrcTableMinCsv.count() 
     var csv_minus_srcTable_count = df_resultCsvMinSrcTable.count()
     
  
     println("Database table minus CSV comparison output using scala except function: ")
     println("Database table minus CSV mismatch count : " + srcTable_minus_csv_count) 
     spark.sql("SELECT * FROM df_resultSrcTableMinCsv" ).show(5,false)
     

     println("CSV VS Database table comparison output using scala except function:")
     println("CSV minus Database table mismatch count : " + csv_minus_srcTable_count)    
     spark.sql("SELECT * FROM df_resultCsvMinSrcTable" ).show(5,false)
     
     
    if((srcTable_minus_csv_count != 0) || (csv_minus_srcTable_count != 0)){
      println("Database data not matched with CSV data - FAIL")
      println("Top 10 Sequence number not matching: ")
      var dbpk: String = pr_lst_db_pks(iterator)
      var csvpk: String = pr_lst_csv_pks(iterator)
      df_resultSrcTableMinCsv.select(dbpk).limit(10).show(5,false)
      df_resultCsvMinSrcTable.select(csvpk).limit(10).show(5,false)
      
      if(isAutoAnalysisRequired){
        println("Below is the dataframe of columns not matching: ")
        var unmatched_cols_dfs:List[DataFrame] = findUnmatchedColumns_5rows(df_resultSrcTableMinCsv, df_resultCsvMinSrcTable, dbpk, csvpk)
        unmatched_cols_dfs.filter(unmatched_cols_df => unmatched_cols_df.count() > 0).foreach(unmatched_cols_df => unmatched_cols_df.show)
      }
      
    }else{
      println("Database data matched with CSV data successfully - PASS")
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