package common
import com.typesafe.config.{Config, ConfigFactory}
import org.slf4j.LoggerFactory

object JsonParser {
  private val logger = LoggerFactory.getLogger(getClass.getName)

  def readJsonFile(): Config={
    logger.warn("Reading JSON File")
    ConfigFactory.load("Config.json")
  }

  def fetchPGTargetTable():String={
    logger.warn("Fetching Target Table")
    val pgTargetSchema = readJsonFile().getString("body.pg_target_schema")
    val pgTargetTable = readJsonFile().getString("body.pg_target_table")
    val pgTarget = pgTargetSchema+"."+pgTargetTable
    logger.warn("Target Table = "+pgTarget)
    pgTarget
  }

  def fetchConfigValue(key: String):String={
    logger.warn("Fetching Config Key")
    val configValue = readJsonFile().getString(key)
    configValue
  }

}
