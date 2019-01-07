import org.apache.spark.sql.SparkSession

object SparkSessionHolder {
  lazy val spark = SparkSession
    .builder
    .master("local")
    .appName("Swapmob")
    .getOrCreate()
}
