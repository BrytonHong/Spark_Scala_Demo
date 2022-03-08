package common
import common.{InputConfig, JsonParser, SparkCommon}

class CommonSpec extends Base {

  behavior of "Spark common"

  it should "create a session" in {
    val inputConfig: InputConfig = InputConfig(env = "dev", targetDB = "pg")
    val spark = SparkCommon.createSparkSession(inputConfig).get
  }

}
