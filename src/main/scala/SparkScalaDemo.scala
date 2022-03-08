import common.{InputConfig, JsonParser, SparkCommon}
import database.SparkDatabase
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.LoggerFactory
import query.SparkTransform

import java.util.Properties

object SparkScalaDemo {

  private val logger = LoggerFactory.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {

    try{
      logger.info("Starting Main")

      if(args.length == 0){
        System.out.println("No Arguments Passed")
        System.exit(1)
      }

      val inputConfig: InputConfig = InputConfig(env = args(0), targetDB = args(1))
      val spark: SparkSession = SparkCommon.createSparkSession(inputConfig).get

      val CourseDF = SparkCommon.readHiveTable(spark).get
      CourseDF.show()

      val transformedDF = SparkTransform.replaceNullValues(CourseDF).get
      transformedDF.show()

      if(inputConfig.targetDB =="pg"){
        val pgTable = JsonParser.fetchPGTargetTable()
        SparkDatabase.writeToPGTable(transformedDF, pgTable)
      }
      else if(inputConfig.targetDB == "hive"){
        val hiveTable = "customerTransformed1"
        SparkCommon.writeDFtoHiveTable(spark,transformedDF,hiveTable)
      }


//      //Streaming Function
//      val schema = StructType(Array(StructField("empId",StringType),StructField("empName",StringType)))
//
//      // Create a "inputDir" under the
//      val streamDF = spark.readStream.option("header","true").schema(schema).csv("D:\\inputDir")
//
//      val query = streamDF.writeStream.format("console").outputMode(OutputMode.Update()).start()
//
//      query.awaitTermination()
    }
    catch {
      case e:Exception=>
        logger.error("Warning :"+e.printStackTrace())
    }

  }
}