import org.apache.spark.sql.functions.udf
import org.apache.kafka.clients.producer.{KafkaProducer, Producer, ProducerRecord}
import java.util._
import spark.implicits._

object kafka_producer
{
  val tableName='Kafka_producer'
  val sConnect = new Utils.sparkConnection
  val spark = sConnect.connect(tableName)
  def main(args: Array[String])={
  
val deBug = args.exists{ x => x.toLowerCase.contains("debug=true") }

      var isSuccess: Boolean = true
      var errorMessage: String = ""
      if (deBug) Utils.printLog(deBug,tableName + ": Debug is Enabled for batchNumber: " + batchNumber)
	  
try {


val props = new Properties
props.put("bootstrap.servers", "sandbox-hdp.hortonworks.com:6667")
props.put("client.id", "Producer")
props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

//visit details--Encrypted user data

val visit_details =spark.read.format("csv")
                   .option("header", "true")
				   .option("inferSchema", "true")
				   .load("/tmp/visit_details.csv")

val producer = new KafkaProducer[String,String](props)

for(a<- visit_details.collect.map(_.toSeq))
(
producer.send(new ProducerRecord[String,String]("Banking_Encrypted",a.mkString(","))) //Base 64 encrypted
)

producer.close();

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
	 println(errorMessage)
    spark.stop
    
  }

  }
}
