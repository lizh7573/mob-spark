import com.graphhopper.matching._
import com.graphhopper._
import com.graphhopper.routing.util.{EncodingManager, CarFlagEncoder}
import com.graphhopper.storage.index.LocationIndexTree
import com.graphhopper.util.GPXEntry

import scala.collection.JavaConverters._

object GraphHopperHelper {

  // Path containging Open Street Map files
  val osmPath = "/home/urathai/Datasets/public/OSM/"

  // Paths to specific Open Street Maps
  val osmPathBeijing = osmPath ++ "Beijing.osm.pbf"
  val osmPathSanFransisco = osmPath ++ "SanFrancisco_-122.449_37.747__-122.397_37.772.osm.pbf"

  // Path to directory for graphHopper to store its data
  val graphHopperPath = "/home/urathai/Datasets/public/graphHopper/graphHopperData"

  /* Used to initialise the GraphHopper data. It reads an OSM file and
   * processes it to create a GraphHopper graph storing it in the
   * GraphHopper data path. This only needs to be done once for every
   * time the mapped used is changed. */
  def init(map: String): Unit = {
    val encoder = new CarFlagEncoder()

    val hopper = new GraphHopper()
      .setStoreOnFlush(true)
      .setEncodingManager(new EncodingManager(encoder))
      .setOSMFile(map)
      .setCHWeightings("shortest")
      .setGraphHopperLocation(graphHopperPath)

    hopper.importOrLoad()
  }

  /* Sets up a GraphHopper object to use for map matching */
  def getHopper = {
    val enc = new CarFlagEncoder() // Vehicle type
    val hopp = new GraphHopper()
      .setStoreOnFlush(true)
      .setCHWeightings("shortest") // Contraction Hierarchy settings
      .setAllowWrites(false)       // Allow multiple object to read
                                   // simultaneously
      .setGraphHopperLocation(graphHopperPath)
      .setEncodingManager(new EncodingManager(enc))

    hopp.importOrLoad()

    (hopp, enc)
  }

  /* Sets up a MapMatching object to use for map matching */
  def getMapMatcher = {
    val (hopp, enc) = GraphHopperHelper.getHopper

    val tripGraph = hopp.getGraphHopperStorage()

    val locationIndex = new LocationIndexMatch(tripGraph,
      hopp.getLocationIndex().asInstanceOf[LocationIndexTree])

    val mm = new MapMatching(tripGraph, locationIndex, enc)

    mm.setSeparatedSearchDistance(600)
    mm.setForceRepair(true)

    mm
  }

  /* Takes a MatchResult from graphhopper and converts it
   * into an Array of Locations.
   */
  def extractLocations(mr: MatchResult): Array[Location] = {
    val pointsList = mr.getEdgeMatches.asScala.zipWithIndex
      .map{ case  (e, i) =>
        if (i == 0)
          e.getEdgeState.fetchWayGeometry(3)   // FetchWayGeometry
                                               // returns vertices on
                                               // graph if 2,
        else
          e.getEdgeState.fetchWayGeometry(2) } // and edges if 3
      .map{case pointList => pointList.asScala.toArray}
      .flatMap{ case e => e}

    val locations = pointsList
      .map(point => Location(Array(point.lon, point.lat)))
      .toArray

    locations
  }

}
