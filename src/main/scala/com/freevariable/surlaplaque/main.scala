package com.freevariable.surlaplaque;

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

import org.apache.spark.mllib.clustering._

import com.freevariable.surlaplaque.importer._
import com.freevariable.surlaplaque.data._
import com.freevariable.surlaplaque.mmp._
import com.freevariable.surlaplaque.app._


object ReplHarness extends Common {
    
    def setup(args: Array[String]) = {
        // XXX: add optional parameters here to support cluster execution
        val app = new SLP(new SparkContext(master, appName))
        
        app.processFiles(SLP.expandArgs(args))
    }
    
    def mapActivities(args: Array[String]) = {
        val data = setup(args)
        
        val apairs = data.map((tp:Trackpoint) => Pair(tp.activity.getOrElse("UNKNOWN"), Pair(tp.timestamp, tp)))
        
        val activities = apairs.keys.distinct.collect
        
        val pairs = for (activity <- activities) yield {
            val filtered = apairs.filter((tup) => {val (a,_) = tup ; a == activity});
            val timestampedTrackpoints = filtered.map((tup) => {val (_,(t,tp)) = tup; (t, tp.watts)}).sortByKey();
            val samples = timestampedTrackpoints.collect;
            Pair(activity, samples)
        }
        
        pairs.toMap
    }
}

object BucketApp extends Common {
    
    def main(args: Array[String]) {
        // XXX: add optional parameters here to support cluster execution
        val app = new SLP(new SparkContext(master, appName))
        
        val data = app.processFiles(SLP.expandArgs(args))
        
        val emptyBuckets = ZoneHistogram.make(ftp)
        
        val counts = data.map((tp:Trackpoint) => (tp.watts)).countByValue
        
        val buckets = counts.foldLeft(emptyBuckets)((b, tup:Pair[Double,Long]) => {
            val (w,ct) = tup
            b.record(w,ct)
            })
        
        Console.println(buckets)
    }
}

object BucketClusterApp extends Common {
    import scala.compat.Platform.currentTime
    
    def activityBuckets(args: Array[String]) = {
        // XXX: add optional parameters here to support cluster execution
        val app = new SLP(new SparkContext(master, appName))
        
        val data = app.processFiles(SLP.expandArgs(args))
        
        val zb = ZoneBuckets.empty
        val zc = ZoneHistogram.makeBucketChooser(ftp)
        
        val tpairs = data.map((tp:Trackpoint) => Pair(Pair(tp.activity.getOrElse("UNKNOWN"), zc(tp.watts)), 1))
        
        val bucketCounts = tpairs.reduceByKey(_ + _)
        val reassociated = bucketCounts.map((tup) => tup match {case ((a,b),c) => (a, (b,c)) })
        
        val pairhists = reassociated.mapValues((pr)=>{ val(b,ct) = pr ; zb.addToBucket(b,ct)})
        val zbcompose = ((z1:ZoneBuckets, z2:ZoneBuckets) => z1 + z2)
        val abuckets = pairhists.foldByKey(ZoneBuckets.empty)(zbcompose)

        abuckets
    }
    
    def run(args: Array[String]) = {
        val abuckets = activityBuckets(args).cache
        
        for ((activity, zb) <- abuckets.collect) {
            val zbp = zb.percentages
            Console.println(s"$activity -> $zbp")
        }
        
        abuckets
    }
    
    def main(args: Array[String]) {
        val numClusters = getEnvValue("SLP_HISTOGRAM_CLUSTERS", "8").toInt
        val numIterations = getEnvValue("SLP_ITERATIONS", "20").toInt
        
		val before = currentTime
		
        val abuckets = activityBuckets(args).cache
        val vectors = abuckets.map((tup) => {val (a,zb) = tup ; zb.percentages})
        
        val km = new KMeans()
        km.setK(numClusters)
        km.setMaxIterations(numIterations)
        
        val model = km.run(vectors)
        
        val labeledVectors = abuckets.map((tup) => {val (act, zb) = tup; (act, model.predict(zb.percentages))})
        val after = currentTime

        Console.println("CLUSTERINGS")
        Console.println("===========\n\n")

        for ((activity, cluster) <- labeledVectors.collect) {
            Console.println(s"$activity is in $cluster")
        }
        
        Console.println("\n")
        Console.println("CLUSTER CENTERS")
        Console.println("===============\n\n")

        for ((center,k) <- model.clusterCenters.view.zipWithIndex) {
            val cstr = center.toList.map(_*100).map("%.1f%%".format(_)).reduce(_ + ", " + _)
            Console.println(s"Cluster $k is centered at $cstr")
        }

        val time_ms = after - before
        Console.println(s"\n\n RUN TOOK $time_ms ms")
    }
}

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