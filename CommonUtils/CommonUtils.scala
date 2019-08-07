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


//Filter records for specific value in a column
def filterDataframe(df: DataFrame, retcolName: String, filtercolName: String, filtercolvalue: String): DataFrame = { 	
	var filterString: String = filtercolName + "='" + filtercolvalue + "'"	
	var newDf = df.select(retcolName).filter(filterString) 
	return newDf
}

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