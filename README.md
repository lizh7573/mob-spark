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
