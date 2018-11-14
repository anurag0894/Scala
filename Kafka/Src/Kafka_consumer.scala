import org.apache.commons.codec.binary.StringUtils
import java.util.{Base64, UUID}
import org.apache.spark.sql.functions.udf
import java.util._
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.StringDeserializer
import scala.collection.mutable.ArrayBuffer
 
object kafka_consumer
{
  val tableName='kafka_consumer'
  val sConnect = new Utils.sparkConnection
  val spark = sConnect.connect(tableName)
  def main(args: Array[String])={
  
val deBug = args.exists{ x => x.toLowerCase.contains("debug=true") }

      var isSuccess: Boolean = true
      var errorMessage: String = ""
      if (deBug) Utils.printLog(deBug,tableName + ": Debug is Enabled for batchNumber: " + batchNumber)
	  
try {
    val properties = new Properties()
    properties.put("bootstrap.servers", "sandbox-hdp.hortonworks.com:6667")
    properties.put("group.id", "Producer")
    properties.put("key.deserializer", classOf[StringDeserializer])
    properties.put("value.deserializer", classOf[StringDeserializer])

    val kafkaConsumer = new KafkaConsumer[String, String](properties)
    kafkaConsumer.subscribe(Arrays.asList("Banking_Encrypted"))
    val arrayBuffer1: ArrayBuffer[String] = ArrayBuffer()

	 val converter=(args:String)=>{
    StringUtils.newStringUtf8(Base64.getDecoder().decode(args))}

    spark.udf.register("converter",converter) //For converting base 64 to UTF string

    while (true) {
      val results = kafkaConsumer.poll(2000)
      for ((topic, data) <- results) {
      arrayBuffer1+=data.split(",").map(x=>converter(x))
      }
    }

insertIntoDB(arrayBuffer1) //Inserting in DB

KafkaConsumer.close();

if (deBug) Utils.printLog(deBug, tableName + ": Processing Completed ...")

}
 catch {
  case e: Throwable => {
          isSuccess=false
          errorMessage=e.getMessage()
      
                
  }
        }
  finally {
    val status = if (isSuccess) "SUCCESS" else "FAILED"
    if (deBug) Utils.printLog(deBug, tableName + ": Writing to audit table with status: " + status)
	 println(errorMessage)
    spark.stop
    
  }

  }
}  

