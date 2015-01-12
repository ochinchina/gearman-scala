name := "GearmanExamples"

version := "1.0"

scalaVersion := "2.11.2"

libraryDependencies += "org.scalatest" % "scalatest_2.11" % "2.2.1" % "test"

libraryDependencies += "com.typesafe.scala-logging" %% "scala-logging" % "3.1.0"

unmanagedJars in Compile += file("../target/scala-2.11/gearman_2.11-1.0.jar")

exportJars := true