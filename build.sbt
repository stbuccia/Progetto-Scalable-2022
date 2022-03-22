
name := "Progetto-Scalable-2022"

version := "0.1"
scalaVersion := "2.13.8"

libraryDependencies ++= Seq (
  "org.apache.spark" %% "spark-core" % "3.2.1",
  "org.apache.spark" %% "spark-mlib" % "1.6.2"
)

resolvers += "Maven" at "https://mvnrepository.com/artifact/org.apache.spark"
