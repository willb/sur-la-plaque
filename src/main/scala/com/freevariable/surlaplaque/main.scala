package com.freevariable.surlaplaque;

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

import org.apache.spark.mllib.clustering._

import com.freevariable.surlaplaque.importer._
import com.freevariable.surlaplaque.data._
import com.freevariable.surlaplaque.mmp._

class SLP(sc: SparkContext) {
    def processFiles(files: Array[String]) = 
        sc.parallelize(files.flatMap((s:String) => extract.trackpointDataFromFile(s)))

    def processFiles(files: Array[String], period: Int) = 
        sc.parallelize(files.flatMap((s:String) => MMP.calculate(extract.trackpointDataFromFile(s).toList, period)))
}

trait Common {
    def master = sys.env.get("SLP_MASTER") match {
        case Some(v) => v
        case None => "local[2]"
    }
    
    def appName = "sur-la-plaque"
    
    def ftp = sys.env.get("SLP_FTP") match {
        case Some(v) => v.toInt
        case None => 300
    }
    
    def getEnvValue(variable:String, default:String) = sys.env.get(variable) match {
        case Some(v) => v
        case None => default
    }
}

object BucketApp extends Common {
    
    def main(args: Array[String]) {
        // XXX: add optional parameters here to support cluster execution
        val app = new SLP(new SparkContext(master, appName))
        
        val data = app.processFiles(args)
        
        val emptyBuckets = ZoneHistogram.make(ftp)
        
        val counts = data.map((tp:Trackpoint) => (tp.watts)).countByValue
        
        val buckets = counts.foldLeft(emptyBuckets)((b, tup:Pair[Double,Long]) => {
            val (w,ct) = tup
            b.record(w,ct)
            })
        
        Console.println(buckets)
    }
}

object ClusterApp extends Common {
    
    def main(args: Array[String]) {
        // XXX: add optional parameters here to support cluster execution
        val app = new SLP(new SparkContext(master, appName))
        
        val data = app.processFiles(args)
        
        val numClusters = getEnvValue("SLP_CLUSTERS", "128").toInt
        val numIterations = getEnvValue("SLP_ITERATIONS", "20").toInt
        
        val vectors = data.map((tp:Trackpoint) => Array(tp.latlong.lat, tp.latlong.lon)).cache()
        val km = new KMeans()
        km.setK(numClusters)
        km.setMaxIterations(numIterations)
        
        val model = km.run(vectors)
        
        val labeledVectors = vectors.map((arr:Array[Double]) => (model.predict(arr), arr))
        
        labeledVectors.countByKey.foreach (kv => println("cluster %d has %d members".format(kv._1,kv._2)))
    }
}

object MMPClusterApp extends Common {
    
    def main(args: Array[String]) {
        // XXX: add optional parameters here to support cluster execution
        val app = new SLP(new SparkContext(master, appName))
        
        val numClusters = getEnvValue("SLP_CLUSTERS", "128").toInt
        val numIterations = getEnvValue("SLP_ITERATIONS", "20").toInt
        val mmpPeriod = getEnvValue("SLP_MMP_PERIOD", "180").toInt
                
        val data = app.processFiles(args, mmpPeriod)

        val vectors = data.map((mtp:MMPTrackpoint) => Array(mtp.mmp)).cache()
        val km = new KMeans()
        km.setK(numClusters)
        km.setMaxIterations(numIterations)
        
        val model = km.run(vectors)
        
        val labeledVectors = vectors.map((arr:Array[Double]) => (model.predict(arr), arr))
        
        labeledVectors.countByKey.foreach (kv => println("cluster %d (center %f) has %d members".format(kv._1,model.clusterCenters(kv._1)(0),kv._2)))
    }
}