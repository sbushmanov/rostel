name := "Scala_Rostel"

version := "0.1"

scalaVersion := "2.12.10"

val sparkVersion = "3.1.2"

resolvers ++= Seq(
  "apache-snapshots" at "https://repository.apache.org/snapshots/"
)

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql"  % sparkVersion % "provided",
  "org.apache.spark" %% "spark-hive" % sparkVersion % "provided",
  "com.github.scopt" %% "scopt" % "4.0.1" % "provided",
  "org.scalatest" %% "scalatest-funsuite" % "3.2.9" % "test",
  "com.github.mrpowers" %% "spark-fast-tests" % "1.0.0" % "test"
)

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}
