name := "myApp"

version := "1.0"

scalaVersion := "2.11.8"

libraryDependencies ++= Seq(
	"org.apache.spark" %% "spark-core" % "2.0.0" % "provided",
	"org.apache.spark" %% "spark-streaming" % "2.0.0" % "provided",
	"org.mongodb" %% "casbah" % "2.8.2",
//	"org.scala-lang" %% "scala-actors" % "2.11.6",
	"com.typesafe.akka" %% "akka-actor" % "2.4-SNAPSHOT",
	"org.apache.spark" %% "spark-streaming-kafka-0-8" % "2.1.0" % "provided"
)

javaOptions in run ++= Seq(
	"-Dlog4j.debug=true",
	"-Dlog4j.configuration=log4j.properties"
)

resolvers += "Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots"
resolvers += "Akka Snapshot Repository" at "http://repo.akka.io/snapshots/"
