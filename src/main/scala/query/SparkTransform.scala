package query
import common.JsonParser
import org.slf4j.LoggerFactory
import org.apache.spark.sql.DataFrame
import reflect.runtime.universe._
import reflect.runtime.currentMirror

object SparkTransform {
  private val logger = LoggerFactory.getLogger(getClass.getName)

  def replaceNullValues(dataFrame: DataFrame): Option[DataFrame]={

    try{
      logger.warn("Read Hive Table method started")
      val strColName = Array("course_name","author_name")

      val transformedDF = dataFrame.na.fill("Unknown",strColName).na.fill(value = "0",Seq("no_of_reviews"))
      Some(transformedDF)
    }
    catch{
      case e: Exception=>
        logger.error("Warning :"+e.printStackTrace())
        System.exit(1)
        None
    }

  }

}
