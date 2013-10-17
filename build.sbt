name := "slp"

version := "0.0.1"

scalaVersion := "2.9.3"

sbtVersion := "0.12.3"

ideaExcludeFolders += ".idea"

ideaExcludeFolders += ".idea_modules"

resolvers += "Akka Repo" at "http://repo.akka.io/repository"

libraryDependencies += "org.apache.spark" % "spark-core_2.9.3" % "0.8.0-incubating"