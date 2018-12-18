class TrajectoryGridTest extends org.scalatest.FunSuite {
  val ex1 = TrajectoryGrid(0, Array())

  val ex2 = TrajectoryGrid(1, Array.range(0, 10, 1).map(i => Grid(i, LocationPartition(Array(i, i)))))

  val ex3 = TrajectoryGrid(2,
    Array
      .range(0, 10, 1)
      .flatMap(i => Array(Grid(2*i, LocationPartition(Array(i, i))),
        Grid(2*i + 1, LocationPartition(Array(i, i))))))

  val ex4 = TrajectoryGrid(3,
    Array
      .range(0, 10, 1)
      .flatMap(i => Array(Grid(2*i, LocationPartition(Array(i, i))),
        Grid(2*i + 1, LocationPartition(Array(i, i))))) ++
      Array
      .range(10, 20, 1)
      .flatMap(i => Array(Grid(2*i, LocationPartition(Array(i - 10, i - 10))),
        Grid(2*i + 1, LocationPartition(Array(i - 10, i - 10)))))
  )

  def arrayEqual(a: Array[LocationPartition], b: Array[LocationPartition]):
      Boolean = {
    a.length == b.length &&
    a.zip(b).forall{
      case (i, j) => i == j
    }
  }

  test("TrajectoryGrid.jumpchain") {
    val jc1 = ex1.jumpchain
    val res1 = Array(): Array[LocationPartition]
    assert(jc1._1 === 0)
    assert(arrayEqual(jc1._2, res1))

    val jc2 = ex2.jumpchain
    val res2 = Array.range(0, 10, 1).map(i => LocationPartition(Array(i, i)))
    assert(jc2._1 === 1)
    assert(!arrayEqual(jc2._2, res1))
    assert(arrayEqual(jc2._2, res2))

    val jc3 = ex3.jumpchain
    val res3 = res2
    assert(jc3._1 === 2)
    assert(arrayEqual(jc3._2, res3))

    val jc4 = ex4.jumpchain
    val res4 = res2 ++ res2
    assert(jc4._1 === 3)
    assert(arrayEqual(jc4._2, res4))
  }

  test("TrajectoryGrid.jumpchainTimes") {
    val jct1 = ex1.jumpchainTimes
    val res1 = Array(): Array[Int]
    assert(jct1._1 === 0)
    assert(java.util.Arrays.equals(jct1._2, res1))

    val jct2 = ex2.jumpchainTimes
    val res2 = Array.range(0, 9, 1).map(i => 1)
    assert(jct2._1 === 1)
    assert(java.util.Arrays.equals(jct2._2, res2))

    val jct3 = ex3.jumpchainTimes
    val res3 = Array.range(0, 9, 1).map(i => 2)
    assert(jct3._1 === 2)
    assert(java.util.Arrays.equals(jct3._2, res3))

    val jct4 = ex4.jumpchainTimes
    val res4 = Array.range(0, 19, 1).map(i => 2)
    assert(jct4._1 === 3)
    assert(java.util.Arrays.equals(jct4._2, res4))
  }
}
