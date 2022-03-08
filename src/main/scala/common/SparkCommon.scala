package common

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.LoggerFactory

object SparkCommon {

  private val logger = LoggerFactory.getLogger(getClass.getName)

  def createSparkSession(inputConfig: InputConfig): Option[SparkSession] ={

    try{

      if(inputConfig.env == "dev"){
        //For Windows
        System.setProperty("hadoop.home.dir", "C:\\winutils")
        //.config("spark.sql.warehouse.dir","file:/D:/Udemy-DataEngineering/SparkHelloWorld/spark-warehouse").enableHiveSupport()
      }

      //Create Spark Session
      val spark = SparkSession
        .builder
        .appName("HelloSpark")
        .config("spark.master","local")
        .enableHiveSupport()
        .getOrCreate()

      logger.info("Starting Spark Session")

      Some(spark)
    }
    catch{
      case e: Exception=>
        logger.error("Warning :"+e.printStackTrace())
        System.exit(1)
        None
    }

  }

  def createHiveTable (spark: SparkSession) : Unit = {
    logger.warn("Create Hive Table method started")

    spark.sql("create database if not exists testdatabase")
    spark.sql("create table if not exists testdatabase.course_table(course_id string, course_name string, author_name string, no_of_reviews string)")
    spark.sql("insert into testdatabase.course_table values (1,'Java1','Tutorial1',41)")
    spark.sql("insert into testdatabase.course_table values (2,'Java2','',42)")
    spark.sql("insert into testdatabase.course_table values (3,'Java3','Tutorial3','')")
    spark.sql("insert into testdatabase.course_table values (4,'Java4','Tutorial4',44)")
    spark.sql("insert into testdatabase.course_table values (5,'Java5','Tutorial5',45)")
    spark.sql("insert into testdatabase.course_table values (6,'Java6','Tutorial6',46)")
    spark.sql("insert into testdatabase.course_table values (7,'Java7','Tutorial7',47)")
    spark.sql("insert into testdatabase.course_table values (8,'Java8','Tutorial8','')")
    spark.sql("insert into testdatabase.course_table values (9,'Java9','Tutorial9',49)")
    spark.sql("insert into testdatabase.course_table values (10,'Java10','',50)")
    spark.sql("alter table testdatabase.course_table set tblproperties('serialization.null.format'='')")
  }

  def readHiveTable (spark: SparkSession) : Option[DataFrame] = {

    try{
      logger.warn("Read Hive Table method started")

      val courseDF = spark.sql("select * from testdatabase.course_table")

      logger.warn("Read Hive Table method ended")

      Some(courseDF)
    }catch{
      case e: Exception=>
        logger.error("Warning :"+e.printStackTrace())
        System.exit(1)
        None
    }
  }

  def writeDFtoHiveTable (spark: SparkSession, dataFrame: DataFrame, hiveTable: String): Unit={

    try{
//      dataFrame.write.format("csv").save("transformedDF")

      val tmpView = hiveTable+"TempView"

      dataFrame.createOrReplaceTempView(tmpView)

      val sparkSQLQuery = "create table if not exists "+hiveTable+" as select * from "+tmpView

      spark.sql(sparkSQLQuery)
    }
    catch{
      case e: Exception=>
        logger.error("Warning :"+e.printStackTrace())
        System.exit(1)
        None
    }

  }

}
