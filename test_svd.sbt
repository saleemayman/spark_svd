name := "testSvdTaxi"

version := "1.0"

scalaVersion := "2.10.4"

libraryDependencies ++= Seq(
    "org.apache.spark"  % "spark-core_2.10"              % "1.5.0" % "provided",
    "org.apache.spark"  % "spark-mllib_2.10"             % "1.3.0",
    "com.github.fommil.netlib"  %   "all"   %   "1.1.2"     pomOnly()
    )

resolvers += "Akka Repository" at "http://repo.akka.io/releases/"

