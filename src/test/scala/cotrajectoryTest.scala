import org.apache.spark.sql.SparkSession
import CoTrajectoryUtils._

class CoTrajectoryTest extends org.scalatest.FunSuite {
  val spark = SparkSessionHolder.spark
  import spark.implicits._

  val cotraj1 = Parse.example("data/test/cotraj-test-1.txt")

  test("CoTrajectoryGrid.swaps") {
    val partitioning = (10L, 0.2)

    val swaps = cotraj1
      .map(_.partitionDistinct(partitioning))
      .swaps(partitioning._1)
      .sort($"time")
      .cache

    assert(swaps.count === 3)

    val swapsArray = swaps.collect

    assert(swapsArray(0).ids.sameElements(Array(0, 1)))
    assert(swapsArray(1).ids.sameElements(Array(1, 2)))
    assert(swapsArray(2).ids.sameElements(Array(0, 2)))
  }
}
