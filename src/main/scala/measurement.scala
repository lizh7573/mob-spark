case class Measurement(time: Int, x: Location) {
  def partition(partitioning: (Double, Double)): Grid = {
    val (timePartitioning, xPartitioning) = partitioning

    val timePartition = (time/timePartitioning).toInt
    val xPartition = x.partition(xPartitioning)

    Grid(timePartition, xPartition)
  }
}

// Holds an id together with a measurement
case class MeasurementID(id: Int, measurement: Measurement) {
  def partition(partitioning: (Double, Double)): GridID = {
    GridID(id, measurement.partition(partitioning))
  }
}

// Holds parition id for time and location
case class Grid(time: Int, x: LocationPartition)

// Holds an id together with a measurement
case class GridID(id: Int, grid: Grid)
