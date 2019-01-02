object TrajectoryHelper {
  // Return the jumpchain list of locations. This is the chain of
  // locations removing any duplicates.
  def jumpchain(locations: Array[LocationPartition]):
      Array[LocationPartition] =
    if (locations.isEmpty)
      Array()
    else
      locations(0) +: locations.sliding(2)
        .collect{ case Array(a, b) if a != b => b }.toArray

  // Return the jumpchain times of a list of grid measurements. This
  // is a list of times where each time represents the time it stays
  // in one particular location.
  def jumpchainTimes(grids: Array[Grid]):
      Array[Int] =
    if (grids.isEmpty)
      Array()
    else
      grids.tail.foldLeft((Array(): Array[Int], grids.head)) {
        case ((ts: Array[Int], g1:Grid), g2:Grid) =>
          if (g1.location != g2.location)
            (ts :+ (g2.time - g1.time), g2)
          else
            (ts, g1)
      }._1

  // Returns the list of transitions for a list of grid measurements.
  // The list of transition consists of pairs of locations together
  // with the time spent in the first location before going to the
  // second.
  def transitions(grids: Array[Grid]):
      Array[(LocationPartition, LocationPartition, Int)] =
    if (grids.length < 2)
      Array()
    else
      grids.tail.foldLeft((Array(): Array[(LocationPartition,
        LocationPartition, Int)], grids.head)) {
        case ((list: Array[(LocationPartition, LocationPartition, Int)],
          g1: Grid), g2: Grid) =>
          if (g1.location != g2.location)
            (list :+ (g1.location, g2.location, g2.time - g1.time), g2)
          else
            (list, g1)
      }._1
}

// Holds a trajectory represented by an id and an array of measurements
case class Trajectory(id: Int, measurements: Array[Measurement]) {
  /* The methods for Trajectory assumes that the array of Measurements
   * is sorted with respect to time. This method makes sure that this
   * is the case.*/
  def normalize() = Trajectory(id, measurements.sortBy(_.time))

  def jumpchain(partitioning: Double): (Int, Array[LocationPartition]) =
    (id, TrajectoryHelper
      .jumpchain(measurements.map(_.location.partition(partitioning))))

  def jumpchainTimes(partitioning: Double): (Int, Array[Int]) =
    (id, TrajectoryHelper
      .jumpchainTimes(measurements
        .map(m => Grid(m.time, m.location.partition(partitioning)))))

  def transitions(partitioning: Double):
      (Int, Array[(LocationPartition, LocationPartition, Int)]) =
    (id, TrajectoryHelper
      .transitions(measurements
        .map(m => Grid(m.time, m.location.partition(partitioning)))))
}

// Holds a trajectory represented by an id and an array of partitions
case class TrajectoryGrid(id: Int, grids: Array[Grid]) {

  /* The methods for TrajectoryGrid assumes that the array of Grids is
   * sorted with respect to time. This method makes sure that this is
   * the case.*/
  def normalize() = TrajectoryGrid(id, grids.sortBy(_.time))

  def jumpchain(): (Int, Array[LocationPartition]) =
    (id, TrajectoryHelper.jumpchain(grids.map(_.location)))

  def jumpchainTimes(): (Int, Array[Int]) =
    (id, TrajectoryHelper.jumpchainTimes(grids))

  def transitions():
      (Int, Array[(LocationPartition, LocationPartition, Int)]) =
    (id, TrajectoryHelper.transitions(grids))
}
