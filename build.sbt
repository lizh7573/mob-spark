name := "Co-traj"

version := "1.0"

sparkVersion := "2.4.0"
scalaVersion := "2.11.12"

sparkComponents += "sql"
sparkComponents += "graphx"

libraryDependencies += "io.spray" %% "spray-json" % "1.3.5"
libraryDependencies += "com.graphhopper" % "map-matching" % "0.7.0"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.5" % Test
libraryDependencies += "com.github.wookietreiber" %% "scala-chart" % "0.5.1"
libraryDependencies += "com.itextpdf" % "itextpdf" % "5.5.6"
