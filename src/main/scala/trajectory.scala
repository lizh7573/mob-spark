// Holds a trajectory represented by an id and an array of measurements
case class Trajectory(id: Int, measurements: Array[Measurement])

// Holds a trajectory represented by an id and an array of partitions
case class TrajectoryGrid(id: Int, grids: Array[Grid]) {

  /* The methods for TrajectoryGrid assumes that the array of Grids is
   * sorted with respect to time. This method makes sure that this is
   * the case.*/
  def normalize() = TrajectoryGrid(id, grids.sortBy(_.time))

  def jumpchain(): (Int, Array[LocationPartition]) = {
    if (grids.isEmpty)
      return (id, Array())
    else {
      val locations = grids.map(_.x)
      val distincLocations = locations(0) +: locations.
        sliding(2).collect{ case Array(a, b) if a != b => b }.toArray

      return (id, distincLocations)
    }
  }

  def jumpchainTimes(): (Int, Array[Int]) = {
    if (grids.isEmpty)
      return (id, Array())
    else {
      val times = grids.tail.foldLeft((Array(): Array[Int], grids.head)) {
        case ((ts: Array[Int], g1:Grid), g2:Grid) =>
          if (g1.x != g2.x)
            (ts :+ (g2.time - g1.time), g2)
          else
            (ts, g1)
      }._1

      return (id, times)
    }
  }
}
