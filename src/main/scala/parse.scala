/* SimpleApp.scala */
import org.apache.spark.sql.SparkSession
import java.text.SimpleDateFormat
import java.util.Date

object Parse {

  val spark = SparkSessionHolder.spark
  import spark.implicits._

  val dataFileSome = "/home/urathai/Datasets/public/T-drive Taxi Trajectories/release/some_data.txt"
  val dataFile = "/home/urathai/Datasets/public/T-drive Taxi Trajectories/release/all_data.txt"

  def tdrive(dataFile: String): org.apache.spark.sql.Dataset[MeasurementID] = {

    val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

    val data = spark.read.textFile(dataFile).map{ line =>
      val parts = line.split(",")
      val id = parts(0).toInt
      val t = (format.parse(parts(1)).getTime/1000).toInt
      val x = Array(parts(2).toDouble, parts(3).toDouble)
      MeasurementID(id, Measurement(t, x))
    }

    return data
  }
}
