name := "myApp"

version := "1.0"

scalaVersion := "2.11.8"

libraryDependencies ++= Seq(
	"org.apache.spark" %% "spark-core" % "2.0.0" % "provided",
	"org.apache.spark" %% "spark-streaming" % "2.0.0" % "provided",
	"org.mongodb" %% "casbah" % "2.8.2",
	"org.apache.spark" %% "spark-streaming-kafka-0-8" % "2.1.0" % "provided"
)

javaOptions in run ++= Seq(
	"-Dlog4j.debug=true",
	"-Dlog4j.configuration=log4j.properties"
)

