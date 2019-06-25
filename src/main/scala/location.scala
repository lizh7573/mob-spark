/* Holds a location. Currently this is stored as an array of doubles
 * which allows both for location on the real line, mainly used for
 * examples, but also for location as longitude and latitude. */
case class Location(x: Array[Double]) {
  override def equals(that: Any): Boolean =
    that match {
      case that: Location => x.sameElements(that.x)
      case _ => false
    }

  override def toString = "[" + x.mkString(", ")+ "]"

  /* Only useful when the location actually represents longitude and
   * latitude */
  def longitude() = x(0)
  def latitude() = x(1)

  /* Return the partition that the location belongs to. The partitioning
   * is given by a uniform grid with the width of each partition given
   * by the argument. */
  def partition(partitioning: Double): LocationPartition =
    LocationPartition(x.map(l => (l/partitioning).toInt))

  /* Return true if the location is inside the box given by its lower
   * and upper boundary in all dimensions. */
  def isInBox(box: Array[(Double, Double)]): Boolean =
    x.length == box.length &&
    x.zip(box).forall{
      case (i, (l, u)) => i <= u && i >= l
    }
}

/* Holds a location partition. This is used to represent a partition
 * of the location space. In the current implementation the
 * partitioning is given by a uniform grid on the location space. */
case class LocationPartition(x: Array[Int]) {
  override def equals(that: Any): Boolean =
    that match {
      case that: LocationPartition => x.sameElements(that.x)
      case _ => false
    }

  override def toString = "[" + x.mkString(", ")+ "]"

  /* Return the location corresponding to the midpoint of the
   * partition. */
  def unpartition(partitioning: Double): Location =
    Location(x.map(i => (i + 0.5)*partitioning))
}
