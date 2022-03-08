package database

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.slf4j.LoggerFactory

import java.util.Properties

object SparkDatabase {

  private val logger = LoggerFactory.getLogger(getClass.getName)

  def createDatabaseConnection(): Properties ={
    //Create Connection with PostgreSQL
    val pgConnectionProperties = new Properties()
    pgConnectionProperties.put("user","postgres")
    pgConnectionProperties.put("password","password")
    logger.info("Database Credentials")
    pgConnectionProperties
  }

  def getPostgresServerDatabase(): String ={
    val pgURL = "jdbc:postgresql://localhost:5432/postgres"
    logger.info("Database Server URL")
    pgURL
  }

  def fetchDataFrameFromPGTable(spark: SparkSession, pgTable: String): Option[DataFrame] ={

    try{
      val pgDataFrame = spark.read.jdbc(getPostgresServerDatabase(),pgTable, createDatabaseConnection())
      logger.info("Database Connection Started")
      Some(pgDataFrame)
    }
    catch{
      case e: Exception =>
        logger.error("Warning :"+e.printStackTrace())
        System.exit(1)
        None
    }
  }

  def writeToPGTable(dataFrame: DataFrame, pgTable: String): Unit ={

    try{

      logger.warn("Writing to PG Table")

      dataFrame.write
        .mode(SaveMode.Append)
        .format("jdbc")
        .option("url",getPostgresServerDatabase())
        .option("dbtable",pgTable)
        .option("user","postgres")
        .option("password","password")
        .save()
      logger.info("Write into Database")
    }
    catch{
      case e: Exception =>
        logger.error("Warning :"+e.printStackTrace())
        System.exit(1)
        None
    }
  }

}
