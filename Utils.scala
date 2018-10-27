import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.DataFrame

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import java.io.File
import java.text.SimpleDateFormat
import java.util.Calendar

object Utils
{
  case class AuditTableSchema(batch_number: String, source_tbl_name: String, des_tbl_name: String, refresh_mode: String, refresh_rec_count: Long,
                             refresh_start_time: String, refresh_end_time: String, refresh_status: String, refresh_runtime: Long, message: String)

  case class AuditRecord (batch_number: String, appIdentifier: String, startTime: Long, recStatus: String, message: String)

  class sparkConnection {

    def connect (appName: String): SparkSession = {

    val warehouseLocation = new File("spark-warehouse").getAbsolutePath
    val spark = SparkSession.builder
           .appName(appName)
           .config("spark.sql.warehouse.dir", warehouseLocation)
           .enableHiveSupport()
           .getOrCreate()
    spark.conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    spark.conf.set("spark.kryo.registrationRequired", "true")
    spark.conf.set("spark.scheduler.mode", "FAIR")
    spark.conf.set("spark.sql.orc.filterPushdown", "true")
    spark.conf.set("spark.storage.StorageLevel","MEMORY_ONLY_SER")
    spark.conf.set("spark.scheduler.mode", "FAIR")
    spark.conf.set("spark.shuffle.file.buffer", "4m")
    return spark
  }
}

def connectnew (appName: String): SparkSession = {

val warehouseLocation = new File("spark-warehouse").getAbsolutePath
val spark = SparkSession.builder
       .appName(appName)
       .config("spark.sql.warehouse.dir", warehouseLocation)       
       .enableHiveSupport()
       .config("hive.exec.dynamic.partition", "true")
       .config("hive.exec.dynamic.partition.mode", "nonstrict")
       .getOrCreate()
spark.conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
spark.conf.set("spark.kryo.registrationRequired", "true")
spark.conf.set("spark.scheduler.mode", "FAIR")
spark.conf.set("spark.sql.orc.filterPushdown", "true")
spark.conf.set("spark.storage.StorageLevel","MEMORY_ONLY_SER")
spark.conf.set("spark.scheduler.mode", "FAIR")
spark.conf.set("spark.shuffle.file.buffer", "4m")

return spark
}

  def printLog(debugLevel: Boolean, message: String) = {
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss")
    if (debugLevel)
         println(dateFormat.format(Calendar.getInstance().getTime()) + ": " + message)
  }

  def auditRecord(nSpark: SparkSession, auditRec: AuditRecord) = {
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss")
    val auditTable =  "g00103.spark_ingestion_audit"
    val insertAudit = AuditTableSchema(auditRec.batch_number, auditRec.appIdentifier, auditRec.appIdentifier, "NA", 0,
                                       dateFormat.format(auditRec.startTime), dateFormat.format(System.currentTimeMillis) , auditRec.recStatus,
                                       (System.currentTimeMillis - auditRec.startTime) / 1000, auditRec.message)
    import nSpark.implicits._
    nSpark.createDataFrame(List(insertAudit)).write.mode("append").insertInto(auditTable)
  }

  def renametable(mSpark: SparkSession, db_name: String, tblname: String) = {
    //assigning values to table name based on input parameter
        val fin_table = tblname
        val temp_table = tblname+"_temp"
        val temp2_table = tblname+"_temp2"

     //replacing content of final table with temp table
          mSpark.sql(s"""alter table $db_name.$fin_table rename to $db_name.$temp2_table""")
          mSpark.sql(s"""alter table $db_name.$temp_table rename to $db_name.$fin_table""")
          mSpark.sql(s"""alter table $db_name.$temp2_table rename to $db_name.$temp_table""")

  }

  def filterArgumentvalues(args:Array[String],fields:Seq[String],separator:String) = {
    val args_map = args.map(arg => {
      val index = arg.indexOf(separator)
      val value = if (index >0) arg.substring(index+separator.size).trim else arg
      val key =  if ( index >0 ) arg.substring(0,index).trim else arg
      (key,value)
    })
      .filter(arg => fields.contains(arg._1)).toMap
    args_map
  }



  def InsertRecord(jSpark: SparkSession, db_name: String, tblname: String , df:DataFrame) = {

        //assigning values to table name based on input parameter
        val fin_table = tblname
        val temp_table = tblname+"_temp"
        val temp2_table = tblname+"_temp2"
        val df_tbl = tblname+"_df"

        //Truncate and re load temp table from transformed DF

        df.createOrReplaceTempView("temp_view")
        jSpark.sql(s"""truncate table $db_name.$temp_table""")
        jSpark.sql(s"""Insert into table $db_name.$temp_table select * from temp_view""")


     //replacing content of final table with temp table
          jSpark.sql(s"""alter table $db_name.$fin_table rename to $db_name.$temp2_table""")
          jSpark.sql(s"""alter table $db_name.$temp_table rename to $db_name.$fin_table""")
          jSpark.sql(s"""alter table $db_name.$temp2_table rename to $db_name.$temp_table""")

  }


    def InsertRecordpartition(lSpark: SparkSession, db_name: String, tblname: String , df:DataFrame,partition_name: String,partition_val: String ) = {

          //assigning values to table name based on input parameter
          val fin_table = tblname
          val temp_table = tblname+"_temp"
          val temp2_table = tblname+"_temp2"
          val df_tbl = tblname+"_df"

          //Insert Overite in given partition

          df.createOrReplaceTempView("temp_view")
          lSpark.sql(s"""INSERT OVERWRITE TABLE  $db_name.$fin_table PARTITION ($partition_name ='$partition_val') select * from temp_view""")
    }

  def generatedf(kspark: SparkSession, sql_query: Array[String] ) : Array[DataFrame] ={
          val dfArray:Array[DataFrame] = for (sql_q<-sql_query) yield {
          val df = kspark.sql(sql_q)
          df
         }
        dfArray
    }

}
