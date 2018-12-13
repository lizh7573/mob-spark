import org.apache.spark.sql.SparkSession

// Holds time and location for one measurement
case class Measurement(time: Int, x: Array[Double])

// Holds an id together with a measurement
case class MeasurementID(id: Int, measurement: Measurement)

// Holds parition id for time and location
case class Grid(time: Int, x: Array[Int])

// Holds an id together with a measurement
case class GridID(id: Int, grid: Grid)

// Holds a trajectory represented by an id and an array of measurements
case class Trajectory(id: Int, measurements: Array[Measurement])

object SparkSessionHolder {
  val spark = SparkSession.builder.appName("Swapmob").getOrCreate()
}
