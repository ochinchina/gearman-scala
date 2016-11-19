name := "Gearman"

version := "1.0"

scalaVersion := "2.11.8"

resolvers += "Scalaz Bintray Repo" at "http://dl.bintray.com/scalaz/releases"

libraryDependencies += "org.scalatest" % "scalatest_2.11" % "2.2.1" % "test"

libraryDependencies += "com.typesafe.scala-logging" %% "scala-logging" % "3.1.0"

libraryDependencies += "org.mockito" % "mockito-core" % "1.9.0" % "test"

libraryDependencies += "org.specs2" %% "specs2" % "2.4.8" % "test"

libraryDependencies += "org.scalaz.stream" %% "scalaz-stream" % "0.5a"

libraryDependencies += "com.typesafe.akka" % "akka-actor_2.11" % "2.4.11"

scalacOptions in (Compile,doc) ++= Seq("-groups", "-implicits")
  
exportJars := true
