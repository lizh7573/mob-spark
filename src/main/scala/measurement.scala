/* Holds a measurement determined by a time and a location. In
 * general the time represents Unix-time. */
case class Measurement(time: Long, location: Location) {
  /* Compute the partition that the measurement belongs to. */
  def partition(partitioning: (Long, Double)): Grid = {
    val (timePartitioning, locationPartitioning) = partitioning

    val timePartition = time/timePartitioning
    val locationPartition = location.partition(locationPartitioning)

    Grid(timePartition, locationPartition)
  }
}

// Holds an id together with a measurement
case class MeasurementID(id: Int, measurement: Measurement) {
  def partition(partitioning: (Long, Double)): GridID = {
    GridID(id, measurement.partition(partitioning))
  }
}

// Holds parition id for time and location
case class Grid(time: Long, location: LocationPartition) {
  /* Return the measurement that is in the middle of the grid. */
  def unpartition(partitioning: (Long, Double)): Measurement =
    Measurement(((time + 0.5)*partitioning._1).toLong,
    location.unpartition(partitioning._2))
}

// Holds an id together with a measurement
case class GridID(id: Int, grid: Grid) {
  def unpartition(partitioning: (Long, Double)): MeasurementID = {
    MeasurementID(id, grid.unpartition(partitioning))
  }
}
