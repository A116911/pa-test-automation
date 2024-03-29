//libraries

import java.text.SimpleDateFormat
import java.util.Calendar
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.{col,lower}
val startTimeMillis = System.currentTimeMillis()


//**************************************************************************************************************************************************

    try{
      dbutils.fs.refreshMounts()
      spark.table("table_list").unpersist()
      println("********** Execute Metadata Comparison of JSON file with DD03L extract **********\n")
    } catch {
      case e: Exception => println(e.getMessage())
    }
    
    val strUsername ="edplztstsa00"
    val strSourceName = "isu"

    val strSAPFilepath = "wasbs://" + strSourceName + "@" + strUsername + ".blob.core.windows.net/dd03l/1/*"
    var strJsonFilePath = "wasbs://edp-etl-pipeline-inputs@" + strUsername + ".blob.core.windows.net/meta/typing/" + strSourceName + "/" + "inputTableName" + "/1/" + strSourceName + "_" + "inputTableName" + "_meta_1.json"
    //var strParquetFilePath = "wasbs://datalake@" + strUsername + ".blob.core.windows.net/sap/" + strSourceName + "/" + "inputTableName" + "/cr/*"
    var tablelistpath = "wasbs://test-parameters@" + strUsername + ".blob.core.windows.net/table_list/json_compare/JSON_Compare.csv"
    
    val format = new SimpleDateFormat("dd/MM/yyyy")
    var runtime = format.format(Calendar.getInstance().getTime()).toString
    var fmtruntime = runtime.replace('/', '-')

    //****************************************************
    // Connect to Test Blob
    //****************************************************
    spark.conf.set("fs.azure.account.key.edplztstsa00.blob.core.windows.net", dbutils.secrets.get(scope = "adbtstkv00", key = "edplztstsa00key"))

    // Load CSV file to DataFrame
    var sap_dd03l_csv = spark.read.format("csv").option("header", "false").option("infer_datetime_format","True").option("delimiter","\u0001").load(s"wasbs://" + strSourceName + "@edplztstsa00.blob.core.windows.net/dd03l/1/*")

    //****************************************************
    //Load table list
    //****************************************************
    var table_list = spark.read.format("csv").option("header", "true").load(tablelistpath).toDF
    table_list.createOrReplaceTempView("table_list")
    spark.table("table_list").cache  
    
    var lst_tables = table_list.select("tablename").filter("system='"+ strSourceName +"'").collect().map(_(0)).toList

    //****************************************************
    // Assign column headers
    //****************************************************
    sap_dd03l_csv = sap_dd03l_csv.toDF("DI_SEQUENCE_NUMBER", "DI_OPERATION_TYPE", "TABNAME", "FIELDNAME", "AS4LOCAL", "AS4VERS", "POSITION", "KEYFLAG", "MANDATORY", "ROLLNAME", "CHECKTABLE", "ADMINFIELD", "INTTYPE", "INTLEN", "REFTABLE", "PRECFIELD", "REFFIELD", "CONROUT", "NOTNULL", "DATATYPE", "LENG", "DECIMALS", "DOMNAME", "SHLPORIGIN", "TABLETYPE", "DEPTH", "COMPTYPE", "REFTYPE", "LANGUFLAG", "DBPOSITION", "ANONYMOUS", "OUTPUTSTYLE", "ODQ_CHANGEMODE", "ODQ_ENTITYCNTR", "DS_UPDATE_TS")

    sap_dd03l_csv.createOrReplaceTempView("sap_dd03l_csv")

    //****************************************************
    //SQLTransform - Transformation of SAP meta data
    //****************************************************

     sap_dd03l_csv = spark.sql("""
     select distinct lower(tablename) as tablename,lower(fieldname) as fieldname,lower(datatype) as datatype,length,targettype from(
          select Base.*,
                    DENSE_RANK() OVER (PARTITION BY tablename, fieldname, as4local, as4vers ORDER BY ds_update_ts DESC,di_sequence_number DESC) AS dense_rank
                    from(
                    select tabname as tablename
                          ,fieldname
                          ,datatype
                          ,case when UPPER(datatype) == "RAW" then 255 
                                --when toUpperCase(datatype) == "TIMS" then 8 
                                when UPPER(datatype) in ("CHAR","CLNT","CUKY","LANG","LCHR","RSTR","SSTR","STRG","NUMC","UNIT") then int(leng)
                           else null end as length
                           ,case when UPPER(datatype) == "DATS" then "date"
                                 when UPPER(datatype) == "FLTP" then  "double"
                                 when UPPER(datatype) in ("INT1","INT2","INT4","PREC") then "integer"
                                 when UPPER(datatype) in ("CURR","D16D","D16R", "D16S","D34D", "D34R","D34S","DEC","QUAN")  then "decimal"
                                 when UPPER(datatype) in ("ACCP","CHAR","CLNT","CUKY","LANG","LCHR","LRAW","NUMC","RAW","REF","RSTR","SSTR","STRG","STRU","TTYP","UNIT")  then "string"
                                 when UPPER(datatype) in ("TIMS") then "time" 
                            else null end as targettype
                            ,di_operation_type
                            ,as4local
                            ,as4vers
                            ,position
                            ,COALESCE(ds_update_ts, CAST('1970-01-01 00:00:00' AS TIMESTAMP)) as ds_update_ts,COALESCE(di_sequence_number, 0) as di_sequence_number  
                    from sap_dd03l_csv 
                    where datatype is not null and lower(tabname) in (select lower(Tablename) from table_list where System= '""" + strSourceName + """')
                  ) Base 
              ) 
              where dense_rank==1 and di_operation_type !='D'
    """)

    sap_dd03l_csv = sap_dd03l_csv.distinct()
    //println("sap_dd03l_csv")
    //sap_dd03l_csv.show(sap_dd03l_csv.count.toInt, false)

    //****************************************************
    // Create final data frame for Sap meta data
    //****************************************************
    
    val selcolumnNameSap = Seq("fieldname", "targettype", "length")
    var dd03l_sap_data = sap_dd03l_csv.select(selcolumnNameSap.map(name => col(name)): _*)
    dd03l_sap_data.sort(asc("fieldname"))
    dd03l_sap_data.createOrReplaceTempView("dd03l_sap_data")
    spark.table("dd03l_sap_data").cache

    //****************************************************
    // Read all Json files
    //****************************************************
    
    val selcolumnNameJason = Seq("name", "type", "length")
    var strsql = ""
    var strsqlUnion = ""

    for (i <- 0 until lst_tables.length) {
      var strJsonFilePathnInner = strJsonFilePath.replaceAll("inputTableName", s"${lst_tables(i)}")
      println("Testing for table: " +lst_tables(i)+ "\n")
      try {
        var df_jsonfile_loop = spark.read
          .option("parserLib", "univocity")
          .option("multiLine", "true")
          .json(strJsonFilePathnInner)
        //df_jsonfile_loop.show()
        var df_JsonTemp_stg = df_jsonfile_loop.select(selcolumnNameJason.map(name => col(name)): _*).filter("name !='di_sequence_number'").filter("name !='di_operation_type'").filter("name !='odq_changemode'").filter("name !='odq_entitycntr'").filter("name !='ds_update_ts'")
      //var df_JsonTemp = df_JsonTemp_stg.withColumnRenamed("table", "tablename").withColumnRenamed("name", "fieldname").withColumnRenamed("type", "targettype")
        var df_JsonTemp = df_JsonTemp_stg.withColumnRenamed("name", "fieldname").withColumnRenamed("type", "datatype").withColumnRenamed("length", "length")
        df_JsonTemp.createOrReplaceTempView("df_JsonTemp_" + lst_tables(i))
        
        //df_JsonTemp.show(df_JsonTemp.count.toInt, false)

        strsql = strsql.concat(" select * from df_JsonTemp_" + lst_tables(i) + " union all ")
      } catch {
        case e: Exception => println(e.getMessage())
      }
    }
    

    strsqlUnion = strsql.substring(0, strsql.length - 10)
    var df_json = spark.sql(strsqlUnion)

    //df_json.show(df_json.count.toInt, false)

    for (i <- 0 until lst_tables.length) {
      spark.catalog.dropTempView("df_JsonTemp_" + lst_tables(i))
    }


    //****************************************************
    // Create final data frame for Json data
    //****************************************************

    var dd03l_json_data = df_json.withColumn("fieldname", lower(col("fieldname"))).withColumn("datatype", lower(col("datatype"))).withColumn("length", lower(col("length")))
    //dd03l_json_data.sort(asc("fieldname"))
    dd03l_json_data.createOrReplaceTempView("dd03l_json_data")
    spark.table("dd03l_json_data").cache

    //dd03l_json_data.show(dd03l_json_data.count.toInt, false)


    //****************************************************
    // Compare Metadata
    //****************************************************
    println("Displaying DD03L Extract: ")
    dd03l_sap_data.show(dd03l_sap_data.count.toInt, false)
    
    println("Displaying Meta Json file: ")
    dd03l_json_data.show(dd03l_json_data.count.toInt, false)
    

    // Subtract DataFrames to get the difference 
    
    println(":::::Meta JSON Comparison with DD03L Extract::::: \n")    
    println("Results of DD03L Extract Minus META JSON File: ")
    var a_b = dd03l_sap_data.select("fieldname", "targettype", "length").except(dd03l_json_data.select("fieldname", "datatype", "length"))
    a_b.show(a_b.count.toInt, false)

    println("Results of META JSON File Minus DD03L Extract: ")
    // Rows present in JSON files but having some mismatch/missing in SAP DD03L files
    var b_a = dd03l_json_data.select("fieldname", "datatype", "length").except(dd03l_sap_data.select("fieldname", "targettype", "length"))
    b_a.show(b_a.count.toInt, false)

    if ((a_b.count.toInt == 0) && (b_a.count.toInt == 0)){
      println("DD03L and Meta JSON comparison is Successful - PASS \n")
    } else {
      println("DD03L and Meta JSON comparison is Usuccessful - FAIL \n")
    }
    
    spark.table("table_list").unpersist()

    println("Script Execution Duration : " + (System.currentTimeMillis() - startTimeMillis) / 1000 + " seconds \n") 