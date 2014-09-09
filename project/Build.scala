import sbt._
import Keys._
import org.scalatra.sbt._
import org.scalatra.sbt.PluginKeys._
import com.mojolly.scalate.ScalatePlugin._
import ScalateKeys._

object SLPBuild  extends Build {
  val SLP_VERSION = "0.0.3"
  
  lazy val analysis = project settings(analysisSettings : _*)

  lazy val viewer = project settings(viewerSettings : _*)
  
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
        "org.json4s" %%  "json4s-jackson" % "3.2.6"
    ),
    scalacOptions ++= Seq("-feature", "-Yrepl-sync", "-target:jvm-1.7")
  )
  
  def sparkSettings = Seq(
    resolvers ++= Seq (
      "Spark 1.1rc3 repository" at "https://repository.apache.org/content/repositories/orgapachespark-1030/"
    ),
    libraryDependencies ++= Seq(
        "org.apache.spark" %% "spark-core" % sparkVersion,
        "org.apache.spark" %% "spark-sql" % sparkVersion,
        "org.apache.spark" %% "spark-mllib" % sparkVersion
    )
  )
  
  def breezeSettings = Seq(
    libraryDependencies ++= Seq(
      "org.scalanlp" %% "breeze" % "0.6"
    )
  )
  
  def testSettings = Seq(
    fork := true,
    libraryDependencies ++= Seq(
      "org.scalacheck" %% "scalacheck" % "1.11.3" % "test"
    )
  )
  
  def dispatchSettings = Seq(
    libraryDependencies += 
      "net.databinder.dispatch" %% "dispatch-core" % "0.11.1"
  )
  
  def scalatraSettings = ScalatraPlugin.scalatraWithJRebel ++ scalateSettings ++ Seq(
    resolvers += "Scalate snapshots" at "https://repository.jboss.org/nexus/content/repositories/fs-snapshots/",
    libraryDependencies ++= Seq(
      "org.scalatra" %% "scalatra" % scalatraVersion,
      "org.scalatra" %% "scalatra-scalate" % scalatraVersion,
      "org.scalatra" %% "scalatra-specs2" % scalatraVersion % "test",
      "org.fusesource.scalate" %% "scalate-core" % scalateVersion,
      "org.fusesource.scalate" %% "scalate-project" % scalateVersion,
      "org.fusesource.scalate" %% "scalate-util" % scalateVersion,
      "ch.qos.logback" % "logback-classic" % "1.0.6" % "runtime",
      "org.eclipse.jetty" % "jetty-webapp" % "8.1.8.v20121106" % "container",
      "org.eclipse.jetty.orbit" % "javax.servlet" % "3.0.0.v201112011016" % "container;provided;test" artifacts (Artifact("javax.servlet", "jar", "jar"))
    ),
    scalateTemplateConfig in Compile <<= (sourceDirectory in Compile){ base =>
      Seq(
        TemplateConfig(
          base / "webapp" / "WEB-INF" / "templates",
          Seq.empty,  /* default imports should be added here */
          Seq(
            Binding("context", "_root_.org.scalatra.scalate.ScalatraRenderContext", importMembers = true, isImplicit = true)
          ),  /* add extra bindings here */
          Some("templates")
        )
      )
    }
  )
  
  def useFixture = {
    sys.env.get("SLP_FIXTURES_FROM") match {
      case Some(dir: String) => s"""
        |val data = app.processFiles(SLP.listFilesInDir("$dir"))
        |data.registerAsTable("trackpoints")
      """.stripMargin
      case _ => ""
    }
  }
  
  def analysisSettings = baseSettings ++ sparkSettings ++ breezeSettings ++ dispatchSettings ++ testSettings ++ Seq(
    initialCommands in console :=
      """
        |import org.apache.spark.SparkConf
        |import org.apache.spark.SparkContext
        |import org.apache.spark.rdd.RDD
        |import com.freevariable.surlaplaque.importer._
        |import com.freevariable.surlaplaque.data._
        |import com.freevariable.surlaplaque.app._
        |val conf = new SparkConf().setMaster("local[8]").setAppName("console").set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        |val sc = new SparkContext(conf)
        |val sqlContext = new org.apache.spark.sql.SQLContext(sc)
        |val app = new SLP(sc)
        |import sqlContext._
        |
      """.stripMargin + useFixture,
    cleanupCommands in console := "app.stop"
  )
  
  def viewerSettings = baseSettings ++ scalatraSettings ++ testSettings
  
  val sparkVersion = "1.1.0"
  val scalatraVersion = "2.2.2"
  val scalateVersion = "1.7.0-SNAPSHOT"
}
