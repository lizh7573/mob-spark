class LocationTest extends org.scalatest.FunSuite {
  test("Location.equals") {
    assert(Location(Array(0)).equals(Location(Array(0))) === true)
    assert(Location(Array(0, 0)).equals(Location(Array(0, 0))) === true)
    assert(Location(Array(1, 2)).equals(Location(Array(1, 2))) === true)

    assert(Location(Array(0)).equals(Location(Array(1))) === false )
    assert(Location(Array(0, 0)).equals(Location(Array(0, 1))) == false)
    assert(Location(Array(0)).equals(Location(Array(0, 0))) == false)
  }

  test("Location.partition") {

    Array.range(0, 10, 1).foreach {
      i => assert(Location(Array(i + 0.5)).partition(1) == LocationPartition(Array(i)))
    }

    Array.range(0, 10, 2).foreach {
      i => assert(Location(Array(i, i + 1, i + 2)).partition(2)
        === LocationPartition(Array(i/2, i/2, i/2 + 1)))
    }
  }

  test("Location.isInBox") {

    Array.range(0, 10, 1).foreach {
      i => assert(Location(Array(i)).isInBox(Array((i - 1, i + 1))))
    }

    Array.range(0, 10, 1).foreach {
      i => assert(Location(Array(i, i + 1)).isInBox(Array((i - 1, i + 1), (i, i + 2))))
    }
  }
}

class LocationPartitionTest extends org.scalatest.FunSuite {
  test("LocationPartition.equals") {
    assert(LocationPartition(Array(0)).equals(LocationPartition(Array(0))) === true)
    assert(LocationPartition(Array(0, 0)).equals(LocationPartition(Array(0, 0))) === true)
    assert(LocationPartition(Array(1, 2)).equals(LocationPartition(Array(1, 2))) === true)

    assert(LocationPartition(Array(0)).equals(LocationPartition(Array(1))) === false )
    assert(LocationPartition(Array(0, 0)).equals(LocationPartition(Array(0, 1))) == false)
    assert(LocationPartition(Array(0)).equals(LocationPartition(Array(0, 0))) == false)
  }
}
