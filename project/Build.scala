import sbt._
import Keys._

object SLPBuild  extends Build {
  val SLP_VERSION = "0.0.2"
  
  lazy val analysis = project settings(analysisSettings : _*)
  
  def baseSettings = Defaults.defaultSettings ++ Seq(
    organization := "com.freevariable.surlaplaque",
    version := SLP_VERSION,
    scalaVersion := "2.10.4",
//    ideaExcludeFolders += ".idea",
//    ideaExcludeFolders += ".idea_modules",
    resolvers ++= Seq(
      "Sonatype Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots/",
      "Sonatype Releases" at "https://oss.sonatype.org/content/repositories/releases/",
      "Akka Repo" at "http://repo.akka.io/repository",
      "spray" at "http://repo.spray.io/"
    ),
    libraryDependencies ++= Seq(
        "com.github.nscala-time" %% "nscala-time" % "0.6.0",
        "io.spray" %%  "spray-json" % "1.2.5",
        "org.json4s" %%  "json4s-jackson" % "3.2.6",
        "org.scalacheck" %% "scalacheck" % "1.11.3" % "test"
    ),
    scalacOptions ++= Seq("-feature", "-Yrepl-sync")
  )
  
  def sparkSettings = Seq(
    libraryDependencies ++= Seq(
        "org.apache.spark" %% "spark-core" % sparkVersion,
        "org.apache.spark" %% "spark-mllib" % sparkVersion
    )
  )
  
  def breezeSettings = Seq(
    libraryDependencies ++= Seq(
      "org.scalanlp" %% "breeze" % "0.6"
    )
  )
  
  def analysisSettings = baseSettings ++ sparkSettings ++ breezeSettings
  
  val sparkVersion = "0.9.1"
}