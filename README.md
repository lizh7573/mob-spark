# Co-Trajectory Analysis
This is a package for performing analysis on collection of
trajectories, co-trajectories.

## Quick Start
Make sure that the following is installed
* [sbt](https://www.scala-sbt.org/)
* [scala](https://www.scala-lang.org/)
* [spark](https://spark.apache.org/)

From the project root build it with

``` shell
sbt package
```

The first time this is done it downloads a lot of packages and can
thus take quite some time. You can perform the, very few, tests with

``` shell
sbt test
```

Most of the tests are in pure Scala and should most likely work. Some
of the tests are using Spark and if these do not work it could be a
problem with you local Spark installation.

To get it running in a local spark-shell use the command

``` shell
spark-shell --jars target/scala-2.11/co-traj_2.11-1.0.jar --packages com.graphhopper:map-matching:0.7.0,io.spray:spray-json_2.11:1.3.5
```

You should then be able to run the first example

```
scala> Examples.example1
Number of trajectories: 5
Number of measurements: 60
Number of possible swaps: 6
Number of possible paths in the DAG: 49
Precomputing data
Computing number of paths for the whole graph
Computing chain of vertices
Computing number of paths trough measurements
Output data about number of paths through measurements to output/example1-1.csv
Number of paths trough the measurement m = MeasurementID(2,Measurement(13,[2.5])): 12
Precomputing data
Computing number of paths between start and end vertex
Number of steps to compute: 5
Every dot is 20 steps, 50 per line
Output data about number of paths starting and ending at a the same vertices as trajectory i to output/example1-2.csv
id = 0: 2
id = 1: 3
id = 2: 2
id = 3: 2
id = 4: 1
```

The other examples will not run since they need access to external
datasets, more information about how to get these can be found further
down.
