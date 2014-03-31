package com.freevariable.surlaplaque;

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
    
    def main(args: Array[String]) {
        // XXX: add optional parameters here to support cluster execution
        val app = new SLP(new SparkContext(master, appName))
        
        val data = app.processFiles(SLP.expandArgs(args))
        
        val numClusters = getEnvValue("SLP_CLUSTERS", "128").toInt
        val numIterations = getEnvValue("SLP_ITERATIONS", "20").toInt
        
        val vectors = data.map((tp:Trackpoint) => Array(tp.latlong.lat, tp.latlong.lon)).cache()
        val km = new KMeans()
        km.setK(numClusters)
        km.setMaxIterations(numIterations)
        
        val model = km.run(vectors)
        
        val labeledVectors = vectors.map((arr:Array[Double]) => (model.predict(arr), arr))
        
        val counts = labeledVectors.countByKey
        
        val maxCount = counts.map({case (_,v) => v}).max
        
        val points = counts.map({case (cluster,count) => makePointMap(cluster, count, model.clusterCenters(cluster), maxCount)})
        
        val struct = Map("type"->"FeatureCollection".toJson, "features"->points.toJson)
        
        val out = outputFile
        out.println(struct.toJson)
        out.close
    }

    def makePointMap(cluster:Int, count:Long, coords:Array[Double], max:Long) = {
        val frac = count.toDouble / max
        val color = rgb(frac)
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
    
    def rgb(frac:Double) = {
        val gb = ((1-frac) * 256).toInt
        "#ff%02x%02x".format(gb,gb)
    }
}

object MMPClusterApp extends Common {
    import com.freevariable.surlaplaque.power.MMPTrackpoint
    
    def main(args: Array[String]) {
        // XXX: add optional parameters here to support cluster execution
        val app = new SLP(new SparkContext(master, appName))
        
        val numClusters = getEnvValue("SLP_CLUSTERS", "128").toInt
        val numIterations = getEnvValue("SLP_ITERATIONS", "20").toInt
        val mmpPeriod = getEnvValue("SLP_MMP_PERIOD", "180").toInt
                
        val data = app.processFiles(SLP.expandArgs(args), mmpPeriod)

        val vectors = data.map((mtp:MMPTrackpoint) => Array(mtp.mmp)).cache()
        val km = new KMeans()
        km.setK(numClusters)
        km.setMaxIterations(numIterations)
        
        val model = km.run(vectors)
        
        val labeledVectors = vectors.map((arr:Array[Double]) => (model.predict(arr), arr))
        
        val out = outputFile
        
        labeledVectors.countByKey.foreach (kv => out.println("cluster %d (center %f) has %d members".format(kv._1,model.clusterCenters(kv._1)(0),kv._2)))
        
        out.close
    }
}