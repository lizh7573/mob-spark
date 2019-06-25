import org.apache.spark.sql.Dataset
import java.text.SimpleDateFormat
import java.util.Date

object Parse {
  val spark = SparkSessionHolder.spark
  import spark.implicits._

  /* Data and methods for parsing and processing data from the T-drive
   * dataset containing trajectories for taxis in Beijing. */

  /* Location of the Beijing data */
  val beijingFile = "/path/to/bejing/data"

  /* Box surrounding the Beijing area, any measurements outside of this
   * box are outside of Beijing. */
  val beijingBox = Array((115.0, 117.0), (39.0, 41.0))

  /* Parse the Beijing data. */
  def beijing(dataFile: String): Dataset[Trajectory] = {

    val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

    val data: Dataset[MeasurementID] = spark.read.textFile(dataFile)
      .map{ line =>
        val parts = line.split(",")
        val id = parts(0).toInt
        val t = (format.parse(parts(1)).getTime/1000).toLong
        val x = Location(Array(parts(2).toDouble, parts(3).toDouble))
        MeasurementID(id, Measurement(t, x))
      }

    CoTrajectoryUtils.getCoTrajectory(data)
  }

  /* Data and methods for parsing and processing data from the Uber
   * dataset containing trajectories for taxis in San Francisco. */

  /* Location of the San Francisco data */
  val sanFranciscoFile = "/path/to/sanfrancisco/data"

  /* Box surrounding the San Francisco area, any measurements outside of
   * this box are outside of San Francisco. */
  val sanFranciscoBox = Array((-122.449, -122.397), (37.747, 37.772))

  /* Parse the San Francisco data. */
  def sanFrancisco(dataFile: String): Dataset[Trajectory] = {

    val format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss")

    val data: Dataset[MeasurementID] = spark.read.textFile(dataFile)
      .map{ line =>
        val parts = line.split("\t")
        val id = parts(0).toInt
        val t = (format.parse(parts(1)).getTime/1000).toLong
        val x = Location(Array(parts(3).toDouble, parts(2).toDouble))
        MeasurementID(id, Measurement(t, x))
      }

    CoTrajectoryUtils.getCoTrajectory(data)
  }

  /* Parse the data for the examples and tests which all use the same
   * format. */
  def example(dataFile: String): Dataset[Trajectory] = {
    val data: Dataset[MeasurementID] = spark.read.textFile(dataFile)
      .map{ line =>
        val parts = line.split(",")
        val id = parts(0).toInt
        val t = parts(1).toLong
        val x = Location(Array(parts(2).toDouble))
        MeasurementID(id, Measurement(t, x))
      }

    CoTrajectoryUtils.getCoTrajectory(data)
  }

}
