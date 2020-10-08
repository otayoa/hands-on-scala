import org.apache.spark.sql.SparkSession

object App {

  def process(spark: SparkSession): Long = {
    val path = "/home/kings/Development/access.log.gz"
    val logs = spark.read.text(path)
    return logs.count()
  }

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().appName("access_log").master("local").getOrCreate()
    val counts = process(spark)
    spark.close()
    println(counts)



  }

}
