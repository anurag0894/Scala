import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.DataFrame
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import java.io.File
import org.apache.spark.storage.StorageLevel
import java.text.SimpleDateFormat
import java.util.Date
import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType,DateType,TimestampType,LongType}
import org.apache.spark.sql.functions.{array, lit, map, concat_ws}
import org.apache.spark.sql.functions.explode




object o_manual_adjustment_flat_to_stg
{

def readCSV(path:String,fileSchema:StructType,isHeader:Boolean,inferSchema:Boolean) :org.apache.spark.sql.DataFrame = {
  val df =  spark.read.format("csv")
              .option("treatEmptyValuesAsNulls",true)	
              .option("ignoreLeadingWhiteSpace" ,true)
              .option("ignoreTrailingWhiteSpace" ,true)
              .option("inferSchema" ,inferSchema)
              .option("header" ,isHeader)
              .option("timestampFormat" ,"yyyy-MM-dd HH:mm:ss")	  
              .schema(fileSchema)
              .load(path)
      df
}


def colType(datatype:String) = {
   datatype.toLowerCase match  { 
  case "string" => org.apache.spark.sql.types.StringType
  case "integer" => org.apache.spark.sql.types.IntegerType
  case "date" => org.apache.spark.sql.types.DateType
  case "short" => org.apache.spark.sql.types.ShortType
  case "long" => org.apache.spark.sql.types.LongType
  case "float" => org.apache.spark.sql.types.FloatType
  case "timestamp" => org.apache.spark.sql.types.TimestampType
  case "boolean" => org.apache.spark.sql.types.BooleanType 
  case _ => org.apache.spark.sql.types.StringType
} 
 }
val spark = SparkSession.builder
      .appName(tableName)
      .config("spark.some.config.option", "config-value")
//    .config("spark.sql.warehouse.dir", warehouseLocation)
//    .config("orc.compress","ZLIB")
      .enableHiveSupport()
      .getOrCreate()

   import spark.implicits._
def main(args: Array[String])={

val deBug = args.exists{ x => x.toLowerCase.contains("debug=true") }

      var isSuccess: Boolean = true
      var errorMessage: String = ""
      val batchNumber = spark.sparkContext.applicationId
      val beforeTimestamp: Long = System.currentTimeMillis()
      val formatter = java.text.NumberFormat.getIntegerInstance
      if (deBug) Utils.printLog(deBug,tableName + ": Debug is Enabled for batchNumber: " + batchNumber)

try {
  
  val schema_create = (arr1:Array[(String,String)]) => {
StructType(arr1.map(row => StructField(row._1,colType(row._2),true)))}

  //Code to read the CSV filelist.csv from HDFS and filtering only XLS and XML contents from the file
    val filelist_schema = StructType(Array(StructField("filename",StringType,true)))
    val filelist_path = "/tmp/manual_adjustments/CSVFiles/filelist.csv"
    val filelist_ds = readCSV(filelist_path,filelist_schema,false,false).as[String]
    val filelist_ds_filtered = filelist_ds.filter( filelist_ds("filename").endsWith("xls") || filelist_ds("filename").endsWith("xml"))
    filelist_ds_filtered.createOrReplaceTempView("mytempTable") 
    //SFTP_NEWFILE_TEST


  //Code to read the CSV indirect.txt from HDFS
    val indirect_schema = StructType(Array(StructField("filename",StringType,true)))
    val indirect_path = "/tmp/manual_adjustments/Indirect.txt"
    val indirect_ds = readCSV(indirect_path,indirect_schema,false,false).as[String] 

  

  
  
 //BACKLOG
 
//Find the number of files that needs to be appended to a tableName
val fileNameArray = indirect_ds.collect
//val fileNameArray = Array("170847938_20172809011208.csv","170962605_20170210075807.csv","171077136_20170610000000.csv")
val csvfilePath = "/tmp/manual_adjustments/CSVFiles/"
val schemaArray = Array(
("field1","String"),
("field2","String") ,
("metric_adjusted","String"), 
("week_no___fw","String"),
("fiscal_year","string"))


val schema = schema_create(schemaArray)


for ( fileName <-fileNameArray ){
    val file_path = csvfilePath + fileName
    val temp_df = readCSV(file_path,schema,true,true)
    temp_df.createOrReplaceTempView("temp_df")
    val sql = s""" insert into g00103.FLAT_FILE select temp_df.*,'$fileName' from temp_df """
    spark.sql(sql)
}



if (deBug) Utils.printLog(deBug, tableName + ": Processing Completed ...")

} catch {
  case e: Throwable => {
          isSuccess=false
          errorMessage=e.getMessage()
                        }
        }
  finally {
    val status = if (isSuccess) "SUCCESS" else "FAILED"
    if (deBug) Utils.printLog(deBug, tableName + ": Writing to audit table with status: " + status)
    Utils.auditRecord(spark, Utils.AuditRecord(batchNumber, tableName, beforeTimestamp, status, errorMessage))
    spark.stop
      }
  }
}
