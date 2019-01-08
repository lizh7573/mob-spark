/* SimpleApp.scala */
import org.apache.spark.sql.SparkSession
import java.text.SimpleDateFormat
import java.util.Date

object Parse {

  val spark = SparkSessionHolder.spark
  import spark.implicits._

  val beijingFileSome = "/home/urathai/Datasets/public/T-drive Taxi Trajectories/release/some_data.txt"
  val beijingFile = "/home/urathai/Datasets/public/T-drive Taxi Trajectories/release/all_data.txt"

  val sanFransiscoFile = "/home/urathai/Datasets/public/San Fransisco/all.tsv"

  def beijing(dataFile: String): org.apache.spark.sql.Dataset[MeasurementID] = {

    val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

    val data = spark.read.textFile(dataFile).map{ line =>
      val parts = line.split(",")
      val id = parts(0).toInt
      val t = (format.parse(parts(1)).getTime/1000).toLong
      val x = Location(Array(parts(2).toDouble, parts(3).toDouble))
      MeasurementID(id, Measurement(t, x))
    }

    return data
  }

  def sanFransisco(dataFile: String):
      org.apache.spark.sql.Dataset[MeasurementID] = {

    val format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss")

    val data = spark.read.textFile(dataFile).map{ line =>
      val parts = line.split("\t")
      val id = parts(0).toInt
      val t = (format.parse(parts(1)).getTime/1000).toLong
      val x = Location(Array(parts(3).toDouble, parts(2).toDouble))
      MeasurementID(id, Measurement(t, x))
    }

    return data
  }
}
