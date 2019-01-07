import com.graphhopper.util.GPXEntry

import spray.json._

import scala.collection.JavaConverters._

import scala.util.{Try, Success, Failure}

import org.apache.spark.sql.functions._

object TrajectoryHelper {
  /* All of the below methods assume that the given list is sorted in
   * time */

  /* Return the jumpchain for a list of locations. This is the chain of
   * locations removing any succesive duplicates. */
  def jumpchain(locations: Array[LocationPartition]):
      Array[LocationPartition] =
    if (locations.isEmpty)
      Array()
    else
      locations(0) +: locations.sliding(2)
        .collect{ case Array(a, b) if a != b => b }.toArray

  /* Return the jumpchain times of a list of grid measurements. This is
   * a list of times where each time represents the time it stays in
   * one particular location. */
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

  /* Return the list of transitions for a list of grid measurements. The
   * list of transition consists of pairs of locations together with
   * the time spent in the first location before going to the
   * second. */
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

/* Holds a trajectory represented by an id and an array of measurements */
case class Trajectory(id: Int, measurements: Array[Measurement]) {
  /* The methods for Trajectory assumes that the array of Measurements
   * is sorted with respect to time. This method makes sure that this
   * is the case.*/
  def normalize(): Trajectory = Trajectory(id, measurements.sortBy(_.time))

  /* Return the jumpchain of the trajectory. This is the chain of
   * locations for the trajectory, removing any succesive
   * duplicates. */
  def jumpchain(partitioning: Double): (Int, Array[LocationPartition]) =
    (id, TrajectoryHelper
      .jumpchain(measurements.map(_.location.partition(partitioning))))

  /* Return the jumpchain times of a trajectory. This is a list of times
   * where each time represents the time the trajectory stays in one
   * particular location. */
  def jumpchainTimes(partitioning: Double): (Int, Array[Int]) =
    (id, TrajectoryHelper
      .jumpchainTimes(measurements
        .map(m => Grid(m.time, m.location.partition(partitioning)))))

  /* Return the list of transitions for a trajectory. The list of
   * transition consists of pairs of locations together with the time
   * spent in the first location before going to the second. */
  def transitions(partitioning: Double):
      (Int, Array[(LocationPartition, LocationPartition, Int)]) =
    (id, TrajectoryHelper
      .transitions(measurements
        .map(m => Grid(m.time, m.location.partition(partitioning)))))

  /* Return a map matched version of the trajectory. The trajectory is
   * matched using Open Street Map data. For more details see the
   * GraphHopperHelper class.*/
  def mapMatch(mm: com.graphhopper.matching.MapMatching =
    GraphHopperHelper.getMapMatcher): Trajectory = {

    val gpxEntries = measurements
      .map{
        m => new GPXEntry(m.location.latitude, m.location.longitude, m.time)}
      .toList
      .asJava

    val mr = Try(mm.doWork(gpxEntries))

    val matchedLocations = mr match {
      case Success(result) => GraphHopperHelper.extractLocations(result)
      case Failure(_) => Array(): Array[Location] // When no match can be made
    }

    /* The time for the measurements are lost in the map matching. Instead
     * of trying to recreate reasonable times we here just use a time
     * starting at 0 and increasing by one for every measurement. */
    val matchedMeasurements = matchedLocations
      .zipWithIndex
      .map{case (location, time) => Measurement(time, location)}

    Trajectory(id, matchedMeasurements)
  }

  import Visualize.LonLatJsonProtocol._

  def toJson(): String =
    Visualize.LonLatJson(coordinates =
      measurements.map(m => (m.location.longitude, m.location.latitude)))
    .toJson
    .prettyPrint

}

/* Holds a trajectory represented by an id and an array of partitions */
case class TrajectoryGrid(id: Int, grids: Array[Grid]) {

  /* The methods for TrajectoryGrid assumes that the array of Grids is
   * sorted with respect to time. This method makes sure that this is
   * the case.*/
  def normalize() = TrajectoryGrid(id, grids.sortBy(_.time))

  /* Return the jumpchain of the trajectory. This is the chain of
   * locations for the trajectory, removing any succesive
   * duplicates. */
  def jumpchain(): (Int, Array[LocationPartition]) =
    (id, TrajectoryHelper.jumpchain(grids.map(_.location)))

  /* Return the jumpchain times of a trajectory. This is a list of times
   * where each time represents the time the trajectory stays in one
   * particular location. */
  def jumpchainTimes(): (Int, Array[Int]) =
    (id, TrajectoryHelper.jumpchainTimes(grids))

  /* Return the list of transitions for a trajectory. The list of
   * transition consists of pairs of locations together with the time
   * spent in the first location before going to the second. */
  def transitions():
      (Int, Array[(LocationPartition, LocationPartition, Int)]) =
    (id, TrajectoryHelper.transitions(grids))
}
