name := "Co-traj"

version := "1.0"

sparkVersion := "3.1.2"
scalaVersion := "2.12.10"

sparkComponents += "sql"
sparkComponents += "graphx"

libraryDependencies += "io.spray" %% "spray-json" % "1.3.5"
libraryDependencies += "com.graphhopper" % "map-matching" % "0.7.0"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.5" % Test
