ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.8"

lazy val root = (project in file("."))
  .settings(
    name := "Progetto-Scalable-2022"
  )

ThisBuild / libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.2.1"