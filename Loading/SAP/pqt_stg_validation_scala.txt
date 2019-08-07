// Declare libraries

import java.util.Properties
import java.text.SimpleDateFormat
import java.util.Calendar
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col,lower}
val startTimeMillis = System.currentTimeMillis()


val strUsername ="edplztstsa00"
val strSourceName = "isu"


try{
  dbutils.fs.refreshMounts()
  spark.table("table_list").unpersist()      
} catch {
    case e: Exception => println(e.getMessage())
}

// CONNECTING TO THE LANDING ZONE
spark.conf.set("fs.azure.account.key.edplztstsa00.blob.core.windows.net", dbutils.secrets.get(scope = "adbtstkv00", key = "edplztstsa00key"))


// PATH TO THE TESTDATA CSV HAVING ALL TEST DATA
var tablelistpath = "wasbs://test-parameters@edplztstsa00.blob.core.windows.net/table_list/pqt_stg/PQT_STG_TestData.csv"


// JDBC CONNECTION DETAILS
val jdbcHostname = "edptstsqldw00.database.windows.net"
val jdbcPort = 1433
val jdbcDatabase = "edptstdb"  
val jdbcUrl = s"jdbc:sqlserver://edptstsqldw00.database.windows.net:1433;database=edptstdb;"

val format = new SimpleDateFormat("dd/MM/yyyy")
var runtime = format.format(Calendar.getInstance().getTime()).toString
var fmtruntime = runtime.replace('/', '-')

var resultspath = "wasbs://test-parameters@" + strUsername + ".blob.core.windows.net/results/" + strSourceName + "/" + "inputTableName" + "/[" + fmtruntime + "]"


// CREATE A PROPERTIES() OBJECT TO HOLD THE PARAMETERS AND CONNECT TO STAGING DB
val connectionProperties = new Properties()

connectionProperties.put("user", s"dataplat")
connectionProperties.put("password", dbutils.secrets.get(scope = "adbtstkv00", key = "sqldw-dataplat-password"))
val driverClass = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
connectionProperties.setProperty("Driver", driverClass)


// *****LOADING TABLE LIST*****
var table_list = spark.read.format("csv").option("header", "true").load(tablelistpath).toDF
table_list.createOrReplaceTempView("table_list")
spark.table("table_list").cache    

var lst_DataSource = table_list.select("system").filter("System='"+ strSourceName +"'").collect().map(_(0)).toList
var lst_tables = table_list.select("Table").filter("System='"+ strSourceName +"'").collect().map(_(0)).toList
var lst_RunDate = table_list.select("RunDate").filter("System='"+ strSourceName +"'").collect().map(_(0)).toList
var lst_TimeStamp = table_list.select("TimeStamp").filter("System='"+ strSourceName +"'").collect().map(_(0)).toList
var lst_DataClassification = table_list.select("DataClassification").filter("System='"+ strSourceName +"'").collect().map(_(0)).toList
var lst_LoadDateTime = table_list.select("LoadDateTime").filter("System='"+ strSourceName +"'").collect().map(_(0)).toList
var lst_RunTime = table_list.select("RunTime").filter("System='"+ strSourceName +"'").collect().map(_(0)).toList

var df_di_parquet = Seq.empty[(String, Long, String)].toDF("Table", "fail_count", "data_value_status")

println("********** Execute data testing for Parquet and Staging files **********\n")
  for (i <- 0 until lst_tables.length) {
    try {
        //println("Iterating " + (i+1) + " of " + lst_tables.length + " tables")
        println("Table Name::" + lst_tables(i))
        println("Table Data Source::" + lst_DataSource(i))
        println("Table Data Classification::" + lst_DataClassification(i))
      
        // ***************************************************************************************************************
        // CONNECT TO AZURE BLOB USING CREDENTIALS
        // ***************************************************************************************************************
		
		spark.conf.set("fs.azure.account.key.edp" + lst_DataClassification(i) + "tstsa00.dfs.core.windows.net", dbutils.secrets.get(scope = "adbtstkv00", key = "edp" + lst_DataClassification(i) + "tstsa00key"))
        spark.conf.set("fs.azure.createRemoteFileSystemDuringInitialization", "true")
		dbutils.fs.ls("abfss://typed@edp" + lst_DataClassification(i) + "tstsa00.dfs.core.windows.net/")
		spark.conf.set("fs.azure.createRemoteFileSystemDuringInitialization", "false")
        
      // CONNECTION TO PARQUET FILE LOCATION
	  
		var df_pqt = spark.read.parquet("abfss://typed@edp"+ lst_DataClassification(i) +"tstsa00.dfs.core.windows.net/" + lst_DataSource(i) + "/" + lst_tables(i) + "/1/delta/" + lst_RunDate(i) + "/" + lst_RunTime(i) + "/*")
	
        // GET PARQUE DATA IN DATAFRAME
           df_pqt.createOrReplaceTempView("pqt")
     
      
        //**********************************
        //ACCESSING THE STAGING TABLE
        //*********************************

          var strStgTableName ="staging.[stg_"+lst_DataSource(i) + "_"+"inputTableName"+"_"+lst_TimeStamp(i)+"]"
          var strStgTableNameInner = strStgTableName.replaceAll("inputTableName", s"${lst_tables(i)}")

          val stgTable = spark.read.jdbc(jdbcUrl, strStgTableNameInner, connectionProperties)
          stgTable.createOrReplaceTempView("stgTable")

          //stgTable.show()
          var df_tableResult = sqlContext.sql("SELECT * FROM stgTable")
      
          // DROPPING TABLE COLUMNS FROM BOTH PARQUE AND STAGING DATAFRAME WHICH WAS INTRODUCED BY DS_JOB FOR COMPARISON
          // SHOW THE PARQUE AND STAGING DATAFRAMES
          // GET THE PARQUE AND STAGING RECORDS
          df_tableResult = df_tableResult.drop("LoadDateTime","RecordSource","LoadAuditId","LoadToolCode") 
          df_tableResult = df_tableResult.drop("zcreation_time","zvalid_from_time","zvalid_to_time","zchanged_time")
          //df_tableResult.show()
          var StgCount = df_tableResult.count()
      
          df_pqt = df_pqt.drop("monotonically_increasing_id","filename")
          df_pqt = df_pqt.drop("_monotonically_increasing_id","_filename")
          df_pqt = df_pqt.drop("zcreation_time","zvalid_from_time","zvalid_to_time","zchanged_time")
      
      
          //df_pqt.show()
          val pqtCount = df_pqt.count()

        // **************************************************************************************
        // STAGING AND PARQUET COUNT COMPARISON
        // **************************************************************************************
          println("************ Execute count comparison between Staging and Parquet ************") 
          println("PARQUE COUNT for " + lst_tables(i) + " is : " + pqtCount + " records ")
          println("STAGING TABLE COUNT for " + lst_tables(i) + " is : " + StgCount + " records ")

          if (StgCount == pqtCount) {
            println("Staging and Parque Table Count has been matched successfully - PASS")
          } else {
            println("Staging and Parque Table Count has not been matched - FAIL")
          }
      
      
        // **************************************************************************************
        // STAGING AND PARQUET DATASET COMPARISON
        // **************************************************************************************
          val df_resultStaging = df_tableResult.except(df_pqt)
          val df_resultPqt = df_pqt.except(df_tableResult)
          df_resultStaging.createOrReplaceTempView("df_resultStaging")
          df_resultPqt.createOrReplaceTempView("ResultPqt")

          println("************ Execute data comparison between Staging and Parquet ************") 

          println("Staging minus Parquet comparison output:")
      
          df_resultStaging.show(df_resultStaging.count.toInt, false)

          val ResultStagingCount = df_resultStaging.count()
          //spark.sql("SELECT count(*) FROM df_resultStaging" ).show()

          println("PQT minus Staging comparison output:")
      
          df_resultPqt.show(df_resultPqt.count.toInt, false)

          val ResultpqtCount = df_resultPqt.count()
          //spark.sql("SELECT count(*) FROM ResultPqt" ).show()
          
          if ((ResultStagingCount == 0) && (ResultpqtCount == 0)) {
            println("Staging and Parque Table Comparison has matched successfully - PASS")
          } else {
            println("Staging and Parque Table Comparison has not matched - FAIL")
          }
      
        // **************************************************************************************
        // STAGING AND PARQUET METADATA VALIDATION
        // **************************************************************************************
          df_pqt.createOrReplaceTempView("pqt")
          df_tableResult.createOrReplaceTempView("stg")
          
          println("***** PQT METADATA *****")
          //df_pqt.printSchema()
          var df_pqt_meta = spark.sql("Describe table pqt")
          df_pqt_meta.createOrReplaceTempView("df_pqt_meta")
          df_pqt_meta.show(df_pqt_meta.count.toInt, false)

          println("***** STG METADATA *****")
          //df_tableResult.printSchema()
          var df_stg_meta = spark.sql("Describe table stg")
          df_stg_meta.createOrReplaceTempView("df_stg_meta")
          df_stg_meta.show(df_stg_meta.count.toInt, false)
      
          
       var df_di_parquet = Seq.empty[(String, Long, String)].toDF("tablename", "fail_count", "data_value_status")
       
        df_di_parquet = df_di_parquet.union(spark.sql("select '" + lst_tables(i) + "' as tablename,count(*) as mis_natch_count, case when count(*) != 0 then 'FAIL' else 'PASS' end as data_value_status from ((select col_name, data_type, comment from df_pqt_meta where col_name not in ('_filename','_index','_errors','_ts')) minus (select col_name, data_type, comment from df_stg_meta where col_name not in ('_filename','_index','_errors','_ts') ))"))
       
       println("Testing for table : " + lst_tables(i)+ "\n")
        
       println("Execute mete data value testing (Parquet - Staging)")
       df_di_parquet.show(df_di_parquet.count.toInt, false)
           
        
    } catch {
      case e: Exception => println(e.getMessage())
    }
  }

  spark.table("table_list").unpersist()

  println("Script Execution Duration : " + (System.currentTimeMillis() - startTimeMillis) / 1000 + " seconds \n") 

//End