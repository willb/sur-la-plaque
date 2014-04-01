package com.freevariable.surlaplaque.app;

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

import org.apache.spark.mllib.clustering._

import com.freevariable.surlaplaque.importer._
import com.freevariable.surlaplaque.data._
// import com.freevariable.surlaplaque.power._
import com.freevariable.surlaplaque.app._

object GPSClusterApp extends Common {
    import spray.json._
    import DefaultJsonProtocol._
    import org.apache.spark.rdd.RDD
    import org.apache.spark.SparkConf
    
    import com.freevariable.surlaplaque.util.ConvexHull
    
    def main(args: Array[String]) {
        val conf = new SparkConf()
                     .setMaster(master)
                     .setAppName(appName)
                     .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

        val app = new SLP(new SparkContext(conf))
        
        val data = app.processFiles(SLP.expandArgs(args))
        
        val numClusters = getEnvValue("SLP_CLUSTERS", "128").toInt
        val numIterations = getEnvValue("SLP_ITERATIONS", "10").toInt
        
        val model = clusterPoints(data, numClusters, numIterations)
        
        val labeledVectors = data.keyBy((tp:Trackpoint) => model.predict(Array(tp.latlong.lat, tp.latlong.lon)))
        
        val hulls = generateHulls(data, model)
        
        val hullPolys = hulls.map({case (cluster, coords) => makeHullPoly(cluster, coords)}).collect
        
        val struct = Map("type"->"FeatureCollection".toJson, "features"->hullPolys.toJson)
        
        val out = outputFile
        out.println(struct.toJson)
        out.close
    }

    def clusterPoints(rdd: RDD[Trackpoint], numClusters: Int, numIterations: Int) = {
      val km = new KMeans()
      km.setK(numClusters)
      km.setMaxIterations(numIterations)
      
      val vecs = rdd.map(tp => Array(tp.latlong.lon, tp.latlong.lat)).cache()
      km.run(vecs)
    }

    def generateHulls(rdd: RDD[Trackpoint], model: KMeansModel) = {
      val clusteredPoints = rdd.groupBy((tp:Trackpoint) => model.predict(Array(tp.latlong.lon, tp.latlong.lat)))
      clusteredPoints.map({case (ctr, pts) => (ctr, ConvexHull.calculate(pts.map(_.latlong).toList))})
    }

    def makeHullPoly(cluster:Int, coords:List[Coordinates]) = {
      val acoords = coords.map(c => Array(c.lon, c.lat).toJson).toArray

      Map(
        "type" -> "Feature".toJson,
        "geometry" -> Map("type"->"Polygon".toJson, "coordinates"->Array((acoords ++ Array(acoords(0))).toJson).toJson).toJson,
        "properties" -> Map("fill"->rgb((cluster*2).toByte, 0, 0), "stroke-width"->"0").toJson
      )
    }

    def makePointMap(cluster:Int, count:Long, coords:Array[Double], max:Long) = {
        val frac = count.toDouble / max
        val color = var_gb(frac)
        val ssize = symsize(frac)
        Map(
        "type" -> "Feature".toJson,
        "geometry" -> Map("type"->"Point".toJson, "coordinates"->Array(coords(1), coords(0)).toJson).toJson,
        "properties" -> Map("marker-color"->color/*, "marker-size"->ssize, "marker-symbol"->"circle"*/).toJson
        )
    }
    
    def symsize(frac:Double) = frac match {
        case x if x < 0.33 => "small"
        case x if x < 0.67 => "medium"
        case _ => "large"
    }
    
    def var_gb(frac:Double) = {
        val gb = ((1-frac) * 256).toInt
        "#ff%02x%02x".format(gb,gb)
    }
    
    def rgb(r: Byte, g: Byte, b: Byte) = "#%02x%02x%02x".format(r,g,b)
}
