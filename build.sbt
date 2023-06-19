ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.8"
  
lazy val root = (project in file("."))
  .settings(
    name := "Progetto-Scalable-2022"
  )

ThisBuild / libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % "3.2.1",
      "org.apache.spark" %% "spark-sql" % "3.2.1",
      "org.apache.spark" %% "spark-mllib" % "3.2.1",
      "org.scalanlp" %% "breeze" % "1.1",
      "org.scalanlp" %% "breeze-natives" % "1.1",
      "org.scalanlp" %% "breeze-viz" % "1.1",
      "log4j" % "log4j" % "1.2.14"
    );

// evilplot
resolvers += Resolver.bintrayRepo("cibotech", "public")
libraryDependencies += "io.github.cibotech" %% "evilplot" % "0.8.1"




/*ThisBuild / libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.2.1"*/
