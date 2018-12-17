case class Location(x: Array[Double]) {
  override def equals(that: Any): Boolean =
    that match {
      case that: Location => java.util.Arrays.equals(x, that.x)
      case _ => false
    }

  def partition(partitioning: Double): LocationPartition =
    LocationPartition(x.map(i => (i/partitioning).toInt))

  def isInBox(box: Array[(Double, Double)]): Boolean =
    x.zip(box).forall{
      case (i, (l, u)) => i <= u && i >= l
    }
}

case class LocationPartition(x: Array[Int]) {
  override def equals(that: Any): Boolean =
    that match {
      case that: LocationPartition => java.util.Arrays.equals(x, that.x)
      case _ => false
    }
}
