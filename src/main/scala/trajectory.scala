object TrajectoryHelper {
  def jumpchain(locations: Array[LocationPartition]):
      Array[LocationPartition] =
    if (locations.isEmpty)
      Array()
    else
      locations(0) +: locations.sliding(2)
        .collect{ case Array(a, b) if a != b => b }.toArray

  def jumpchainTimes(grids: Array[Grid]):
      Array[Int] =
    if (grids.isEmpty)
      Array()
    else
      grids.tail.foldLeft((Array(): Array[Int], grids.head)) {
        case ((ts: Array[Int], g1:Grid), g2:Grid) =>
          if (g1.x != g2.x)
            (ts :+ (g2.time - g1.time), g2)
          else
            (ts, g1)
      }._1

  def transitions(grids: Array[Grid]):
      Array[(LocationPartition, LocationPartition, Int)] =
    if (grids.length < 2)
      Array()
    else
      grids.tail.foldLeft((Array(): Array[(LocationPartition,
        LocationPartition, Int)], grids.head)) {
        case ((list: Array[(LocationPartition, LocationPartition, Int)],
          g1: Grid), g2: Grid) =>
          if (g1.x != g2.x)
            (list :+ (g1.x, g2.x, g2.time - g1.time), g2)
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
      .jumpchain(measurements.map(_.x.partition(partitioning))))

  def jumpchainTimes(partitioning: Double): (Int, Array[Int]) =
    (id, TrajectoryHelper
      .jumpchainTimes(measurements
        .map(m => Grid(m.time, m.x.partition(partitioning)))))

  def transitions(partitioning: Double):
      (Int, Array[(LocationPartition, LocationPartition, Int)]) =
    (id, TrajectoryHelper
      .transitions(measurements
        .map(m => Grid(m.time, m.x.partition(partitioning)))))
}

// Holds a trajectory represented by an id and an array of partitions
case class TrajectoryGrid(id: Int, grids: Array[Grid]) {

  /* The methods for TrajectoryGrid assumes that the array of Grids is
   * sorted with respect to time. This method makes sure that this is
   * the case.*/
  def normalize() = TrajectoryGrid(id, grids.sortBy(_.time))

  def jumpchain(): (Int, Array[LocationPartition]) =
    (id, TrajectoryHelper.jumpchain(grids.map(_.x)))

  def jumpchainTimes(): (Int, Array[Int]) =
    (id, TrajectoryHelper.jumpchainTimes(grids))

  def transitions():
      (Int, Array[(LocationPartition, LocationPartition, Int)]) =
    (id, TrajectoryHelper.transitions(grids))
}
