name := "slp"

version := "0.0.1"

scalaVersion := "2.10.3"

sbtVersion := "0.13.1"

ideaExcludeFolders += ".idea"

ideaExcludeFolders += ".idea_modules"

resolvers += "Akka Repo" at "http://repo.akka.io/repository"

libraryDependencies ++= Seq(
    "org.apache.spark" % "spark-core_2.10" % "0.9.1",
    "org.apache.spark" % "spark-mllib_2.10" % "0.9.1"
)

libraryDependencies += "com.github.nscala-time" %% "nscala-time" % "0.6.0"

resolvers += "spray" at "http://repo.spray.io/"

libraryDependencies += "io.spray" %%  "spray-json" % "1.2.5"

libraryDependencies += "org.json4s" %%  "json4s-jackson" % "3.2.6"


// Breeze options

resolvers ++= Seq(
            "Sonatype Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots/",
            "Sonatype Releases" at "https://oss.sonatype.org/content/repositories/releases/",
            "Spark snapshots" at "https://repository.apache.org/content/repositories/orgapachespark-1009/"
)

libraryDependencies += "org.scalanlp" %% "breeze" % "0.6"

scalacOptions ++= Seq("-feature")