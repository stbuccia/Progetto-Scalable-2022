import sbt.project
lazy val root = (project in file("."))
  .settings(
    name := "Progetto-Scalable-2022",
    version := "Progetto-Scalable-2022",
    scalaVersion := "2.13.8",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % "3.2.1",
      "org.apache.spark" %% "spark-sql" % "3.2.1",
      "org.apache.spark" %% "spark-mllib" % "3.2.1",
    ),
  )