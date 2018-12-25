class TrajectoryHelperTest extends org.scalatest.FunSuite {
  def arrayEqual[T](a: Array[T], b: Array[T]):
      Boolean = {
    a.length == b.length &&
    a.zip(b).forall{
      case (i, j) => i == j
    }
  }

  // Empty array
  val locations1 = Array(): Array[LocationPartition]

  // [(0, 0), (1, 1), ..., (9, 9)]
  val locations2 = Array.range(0, 10, 1)
    .map(i => LocationPartition(Array(i, i)))

  // [(0, 0), (0, 0), (1, 1), (1, 1), ..., (9, 9), (9, 9)]
  val locations3 = locations2.flatMap(i => Array(i, i))

  // [(0, 0), (0, 0), (1, 1), (1, 1), ..., (9, 9), (9, 9),
  //  (9, 9), (9, 9), (8, 8), (8, 8), ..., (0, 0), (0, 0)]
  val locations4 = locations3 ++ locations3.reverse

  test("TrajectoryHelper.jumpchain") {
    val jc1 = TrajectoryHelper.jumpchain(locations1)
    val res1 = locations1
    assert(arrayEqual(jc1, res1))

    val jc2 = TrajectoryHelper.jumpchain(locations2)
    val res2 = locations2
    assert(arrayEqual(jc2, res2))

    val jc3 = TrajectoryHelper.jumpchain(locations3)
    val res3 = locations2
    assert(arrayEqual(jc3, res3))

    val jc4 = TrajectoryHelper.jumpchain(locations4)
    val res4 = locations2 ++ locations2.reverse.tail
    assert(arrayEqual(jc4, res4))
  }

  val grids1 = Array(): Array[Grid]

  val grids2 = locations2.zipWithIndex.map{
    case (x, t) => Grid(t, x)
  }

  val grids3 = locations3.zipWithIndex.map{
    case (x, t) => Grid(t, x)
  }

  val grids4 = locations4.zipWithIndex.map{
    case (x, t) => Grid(t, x)
  }

  test("TrajectoryHelper.jumpchainTimes") {
    val jct1 = TrajectoryHelper.jumpchainTimes(grids1)
    val res1 = Array(): Array[Int]
    assert(java.util.Arrays.equals(jct1, res1))

    val jct2 = TrajectoryHelper.jumpchainTimes(grids2)
    val res2 = Array.fill(9)(1)
    assert(java.util.Arrays.equals(jct2, res2))

    val jct3 = TrajectoryHelper.jumpchainTimes(grids3)
    val res3 = Array.fill(9)(2)
    assert(java.util.Arrays.equals(jct3, res3))

    val jct4 = TrajectoryHelper.jumpchainTimes(grids4)
    val res4 = Array.fill(9)(2) ++ (4 +: Array.fill(8)(2))
    assert(java.util.Arrays.equals(jct4, res4))
  }

  test("TrajectoryHelper.transitions") {
    val transitions1 = TrajectoryHelper.transitions(grids1)
    val res1 = Array(): Array[(LocationPartition, LocationPartition, Int)]
    assert(arrayEqual(transitions1, res1))

    val transitions2 = TrajectoryHelper.transitions(grids2)
    val res2 = locations2.sliding(2).map(s => (s(0), s(1), 1)).toArray
    assert(arrayEqual(transitions2, res2))

    val transitions3 = TrajectoryHelper.transitions(grids3)
    val res3 = locations2.sliding(2).map(s => (s(0), s(1), 2)).toArray
    assert(arrayEqual(transitions3, res3))

    val transitions4 = TrajectoryHelper.transitions(grids4)
    val res4 = (locations2.sliding(2).map(s => (s(0), s(1), 2)).toArray ++
      ((LocationPartition(Array(9, 9)), LocationPartition(Array(8, 8)), 4) +:
        locations2.reverse.tail.sliding(2).map(s => (s(0), s(1), 2)).toArray))
    assert(arrayEqual(transitions4, res4))
  }
}
