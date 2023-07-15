import sbt.project
lazy val root = (project in file("."))
  .settings(
    name := "Progetto-Scalable-2022",
    version := "Progetto-Scalable-2022",
    scalaVersion := "2.12.14",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % "3.2.1" %"provided",
      "org.apache.spark" %% "spark-sql" % "3.2.1" % "provided",
      "org.apache.spark" %% "spark-mllib" % "3.2.1" % "provided",
      "org.scalanlp" %% "breeze" % "1.1",
      "org.scalanlp" %% "breeze-natives" % "1.1",
      "org.scalanlp" %% "breeze-viz" % "1.1",
      "log4j" % "log4j" % "1.2.14",
    )
);

// evilplot
resolvers += Resolver.bintrayRepo("cibotech", "public")
libraryDependencies += "io.github.cibotech" %% "evilplot" % "0.8.1"

/*set mainClass in (Compile, run) := Some("Foo")*/

/*lazy val app = (project in file("app"))*/
  /*.settings(    assembly / mainClass := Some("Main"),*/
  /*)*/

assemblyMergeStrategy in assembly := {
 case PathList("META-INF", _*) => MergeStrategy.discard
 case _                        => MergeStrategy.first
}

