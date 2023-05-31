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
      "org.scalanlp" %% "breeze" % "1.1",
      "org.scalanlp" %% "breeze-natives" % "1.1",
      "org.scalanlp" %% "breeze-viz" % "1.1"
    ),
  )

// evilplot
resolvers += Resolver.bintrayRepo("cibotech", "public")
libraryDependencies += "io.github.cibotech" %% "evilplot" % "0.8.1"

//ThisBuild / version := "0.1.0-SNAPSHOT"
//
//ThisBuild / scalaVersion := "2.13.8"
//
//lazy val root = (project in file("."))
//  .settings(
//    name := "Progetto-Scalable-2022"
//  )
//
//ThisBuild / libraryDependencies ++= Seq(
//      "org.apache.spark" %% "spark-core" % "3.2.1",
//      "org.apache.spark" %% "spark-sql" % "3.2.1",
//      "org.apache.spark" %% "spark-mllib" % "3.2.1",
//      "org.scalanlp" %% "breeze" % "1.1",
//      "org.scalanlp" %% "breeze-natives" % "1.1",
//      "org.scalanlp" %% "breeze-viz" % "1.1",
//      "com.cibo" %% "evilplot" % "0.6.3"
//    ),
//
//
//
///*ThisBuild / libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.2.1"*/
