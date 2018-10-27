import org.apache.spark.sql.internal.HiveSerDe
import scala.collection.mutable.ListBuffer


//update_definitions

/*
table_name--table_name to update.
batch_number--one batch number update will get fired only once.
partition_column--partitioned on the basis of which you want to update
temporary_partition--for error recovery
from_clause-- from <tables and conditions>
case_when--when <conditions> then <statement>
*/

object o_update_definitions {
  var sqlString = ""
  var progressValue = ""
 
  case class record(colToUpdate: String, colUpdateCondition: String)
 
  def main(args: Array[String]): Unit = {
 
    val deBug = args.exists{ x => x.toLowerCase.contains("debug=true") }
    val displayOnly = args.exists{ x => x.toLowerCase.contains("displayonly=true") }
    val andClause = " and " + args.find(_.startsWith("table_name=")).getOrElse("1=1") +
                    " and " + args.find(_.startsWith("batch_number=")).getOrElse("1=1")
 
    var isSuccess: Boolean = true
    var errorMessage: String = ""
 
    val batchNumber = spark.sparkContext.applicationId
    val beforeTimestamp: Long = System.currentTimeMillis()
    val formatter = java.text.NumberFormat.getIntegerInstance
    var recordSet = new ListBuffer[record]()
    var columnValues = new  ListBuffer[String]()
 
 
    if (deBug) Utils.printLog(deBug,tableName + ": Debug is Enabled for batchNumber: " + batchNumber)
    try {
         spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")
         spark.sql("set hive.exec.dynamic.partition=true")
         spark.conf.set("spark.sql.hive.manageFilesourcePartitions","false")

        //Get Data from the update_definitions
        sqlString = "select distinct table_name, batch_number, partition_column, temporary_partition, from_clause " +
                    " from g00103.update_definitions  " +
                    " where enabled_flag = 'Y' order by table_name, batch_number"
                     
        val df_def = spark.sql(sqlString).collect
 
        for (rec <- df_def) {
            val sourceTableName = rec(0).toString
            val tmpTableName = sourceTableName + "_tmp"
            val batchNumber = rec(1).toString
            val sourcePartitionColumn = rec(2).toString
            val newPartitionColumn = rec(3).toString
            val from_clause = Option(rec(4)).getOrElse("").toString
            val fromClause = if (from_clause == "") sourceTableName else from_clause
            val sourcePartitionColumnNoQuotes = sourcePartitionColumn.replaceAll("'","")
            if (deBug) Utils.printLog(deBug,tableName + ": Processing Table: " + sourceTableName + " Batch: " + batchNumber + " Parition: " + sourcePartitionColumn)
            progressValue = "FOR_LOOP_WITH_KEY_VARIABLE_CONFIGURED"
 
            //Get column and its value from update_definitions for the table above.
            sqlString = "select lower(update_column) update_column, case_when from g00103.update_definitions " +
                        "where table_name = '" + sourceTableName + "'" +
                        " and batch_number = " + batchNumber +
                        " and enabled_flag = 'Y' " +
                        " and regexp_replace(partition_column,decode(unhex(hex(39)), 'US-ASCII'),'') = '" + sourcePartitionColumnNoQuotes + "'"  //no quotes
            val df_cols = spark.sql(sqlString).collect
            recordSet.clear
            columnValues.clear

            //Get update columns and their corresonding conditions for update for this batch
            for (colBuffer <- df_cols) {
                recordSet += record(colBuffer(0).toString, colBuffer(1).toString)
                columnValues += colBuffer(0).toString
            }
            
            //Check for duplicate columns in the table
            if (columnValues.size != columnValues.distinct.size) {
                println("Error: Duplicate Columns found in g00103.update_definitions table for table name " + sourceTableName + " for batch number " + batchNumber + " in partition " + sourcePartitionColumn)
                System.exit(1)
            }
            progressValue = "FOUND_ALL_SOURCE_TABLE_COLUMNS"

            //DataFrame to get all columns from source table
            val df = spark.sql("select * " + " from " + sourceTableName + " where " +  sourcePartitionColumn)
            val cols = df.columns
            val originalCount = df.count
 
            //Build the transformed String
            sqlString = ""
            var counter = 0
            for (col <- cols) {
                counter += 1
                val colSet = recordSet.filter(_.colToUpdate.equals(col))
                if (colSet.isEmpty) //Column not foud in ListBuffer
                   sqlString += col + ", "
                else
                   sqlString += " case " + colSet(0).colUpdateCondition + " else " + colSet(0).colToUpdate + " end as " + colSet(0).colToUpdate + ", "
            }
            if (deBug) Utils.printLog(deBug,tableName + ": Column Count in Temp View: " + counter)
            sqlString = sqlString.substring(0,sqlString.length - 2)
            
            //Create temp view from source Table

            sqlString = "select " + sqlString + " from " + fromClause + " where " +  sourcePartitionColumn 
            val dfNew = spark.sql(sqlString)
            
            //Check for count from original table parition should match the new temp table
            val newCount = dfNew.count
            if (originalCount != newCount) {
                println("SQL is incorrect for Update: ")
                println(sqlString)
                println("Error Processing Table: " + sourceTableName + " Batch: " + batchNumber + " Parition: " + sourcePartitionColumn)
                println("Original record count : " + originalCount + " does not match transformed count: " + newCount)
                System.exit(1)
            }

            if (displayOnly) 
                println("Write to Temp Table " + tmpTableName + " : " + sqlString)
            else
                dfNew.repartition(10).write.format("orc").mode("overwrite").saveAsTable(tmpTableName)
            if (deBug) Utils.printLog(deBug, tableName + " Temp View Created: " + formatter.format(dfNew.count))
            progressValue = "COLUMN_TRANSFORMATION_COMPLETE"
 
            spark.conf.set("spark.sql.hive.manageFilesourcePartitions","true")
 
            //Rename selected partition from main table
            sqlString = "alter table " + sourceTableName + " partition (" + sourcePartitionColumn + ") rename to partition (" + newPartitionColumn + ")" //no quotes
            if (deBug) Utils.printLog(deBug, tableName + " Partition Renamed : " + sourceTableName + " Partition: " + sourcePartitionColumn + " to " + newPartitionColumn)
            if (displayOnly) 
                println("Rename Partition: " + sqlString)
            else
                spark.sql(sqlString).collect
            progressValue = "PARTITION_RENAMED"
 
            spark.sql("set hive.exec.dynamic.partition.mode=nonstrict").collect
 
            //insert into new table from temp view with transformed data
            sqlString = "insert into " +  sourceTableName + " select * from " + tmpTableName
            if (deBug) Utils.printLog(deBug, tableName + " Insert into source table: " + sqlString)
            if (displayOnly) 
                println("Insert into Target: " + sqlString)
            else {
                spark.sql(sqlString).collect
                spark.sql("drop table " + tmpTableName)                
            }                
            progressValue = "INSERT_TRANSFORMED_DATA_TO_SOURCE"
 
            //Drop Temp Partition
            sqlString = "alter table " + sourceTableName + " drop partition (" + newPartitionColumn + ")"
            if (deBug) Utils.printLog(deBug, tableName + " Partition Dropped : " + sourceTableName + " Partition: " + newPartitionColumn)
            if (displayOnly) 
                println("Drop Partition: " + sqlString)
            else
                spark.sql(sqlString).collect
            progressValue = "DROP_TEMP_PARTITION"
        }
    } catch {
          case e: Throwable => {
            isSuccess = false
            errorMessage = e.getMessage()
            if (deBug) Utils.printLog(deBug, tableName + ": Error in Processing: " + errorMessage +
                 "\n Last SQL: " + sqlString + "\n progressValue: " + progressValue)
           }
        }
    finally {
      val status = if (isSuccess) "SUCCESS" else "FAILED"
      if (deBug) Utils.printLog(deBug, tableName + ": Writing to audit table with status: " + status)
      Utils.auditRecord(spark, Utils.AuditRecord(batchNumber, tableName, beforeTimestamp, status, errorMessage))
      spark.stop
    }
  } //End of main function
 
} // End
 
 
