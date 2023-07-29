import sbt.project

ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.14"

lazy val root = (project in file("."))
  .settings(
    name := "association-rule-mining-earthquake-prediction",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % "3.2.2",
      "org.apache.spark" %% "spark-sql" % "3.2.2",
      "org.apache.spark" %% "spark-mllib" % "3.2.2"
      /*"org.apache.spark" %% "spark-core" % "3.2.2" % "provided",*/
      /*"org.apache.spark" %% "spark-sql" % "3.2.2" % "provided",*/
      /*"org.apache.spark" %% "spark-mllib" % "3.2.2" % "provided",  */
    ),

    assembly / assemblyJarName := "armep.jar",
    assembly / assemblyMergeStrategy := {
      case PathList("META-INF", xs@_*) => MergeStrategy.discard
      case x => MergeStrategy.first
    },

    artifactName := {(sv: ScalaVersion, module: ModuleID, artifact: Artifact) => "armep.jar"}


  )


// evilplot
resolvers += Resolver.bintrayRepo("cibotech", "public")
libraryDependencies += "io.github.cibotech" %% "evilplot" % "0.8.1"

// log4j
libraryDependencies += "log4j" % "log4j" % "1.2.14"
