/* SimpleApp.scala */
import org.apache.spark.sql.SparkSession
import java.text.SimpleDateFormat
import java.util.Date

object Parse {

  val spark = SparkSession.builder.appName("Parser").getOrCreate()

  import spark.implicits._

  val dataFile = "/home/urathai/Datasets/public/T-drive Taxi Trajectories/release/all_data.txt"

  def tdrive(dataFile: String): org.apache.spark.sql.Dataset[Measurement] = {

    val format = new SimpleDateFormat("YYYY-MM-dd HH:mm:ss")

    val data = spark.read.textFile(dataFile).map{ line =>
      val parts = line.split(",")
      val id = parts(0).toInt
      val t = (format.parse(parts(1)).getTime/1000).toInt
      val x = Array(parts(2).toDouble, parts(3).toDouble)
      Measurement(id, t, x)
    }

    return data
  }
}
