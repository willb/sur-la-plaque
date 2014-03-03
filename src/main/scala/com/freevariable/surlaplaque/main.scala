package com.freevariable.surlaplaque;

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

import org.apache.spark.mllib.clustering._

import com.freevariable.surlaplaque.importer._
import com.freevariable.surlaplaque.data._
// import com.freevariable.surlaplaque.power._
import com.freevariable.surlaplaque.app._

object ReplHarness extends Common {
    
    def setup(args: Array[String], providedApp: Option[SLP] = None) = {
        // XXX: add optional parameters here to support cluster execution
        val app = providedApp.getOrElse(new SLP(new SparkContext(master, appName)))
        
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

object WaveletClusterApp extends Common {
    import scala.compat.Platform.currentTime
    import org.apache.spark.rdd.RDD
    import com.freevariable.surlaplaque.wavelets._
    import com.freevariable.surlaplaque.power.NP
    import org.apache.spark.SparkConf
    
    val OFFSET = 30
    val KEEP = 0.15
    
    def processActivities(args: Array[String]) = {
        val conf = new SparkConf()
                     .setMaster(master)
                     .setAppName(appName)
                     .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

        val app = new SLP(new SparkContext(conf))
        
        val data = ReplHarness.setup(args, Some(app))
        
        val apairs = data.map((tp:Trackpoint) => Pair(tp.activity.getOrElse("UNKNOWN"), Pair(tp.timestamp, tp)))
        
        val activities = apairs.keys.distinct.collect
        
        // XXX: this is somewhat bogus
        val pairs = for (activity <- activities) yield {
            val filtered = apairs.filter((tup) => {val (a,_) = tup ; a == activity})
            val timestampedTrackpoints = filtered.map((tup) => {val (_,(t,tp)) = tup; (t, tp.watts)}).sortByKey()
            val samples = timestampedTrackpoints.collect
            Pair(activity, samples.map((ttp) => ttp._2))
        }
        
        app.context.parallelize(pairs)
    }
    
    def transformWavelets(rdd: RDD[(String, Array[Double])], keep: Double) = {
        rdd.flatMap((pair) => {
            val (activity, samples) = pair
            for (((wavelet, sampleWindow),idx) <- (WaveletExtractor.transformAndAbstract(samples, skip=OFFSET, keepRatio=keep).zipWithIndex)) yield ((activity,idx), wavelet, sampleWindow)
        })
    }
    
    def findClusters(awpairs: RDD[((String, Int), Array[Double])]) = {
        val numClusters = getEnvValue("SLP_WAVELET_CLUSTERS", "24").toInt
        val numIterations = getEnvValue("SLP_ITERATIONS", "50").toInt

        val km = new KMeans()
        km.setK(numClusters)
        km.setMaxIterations(numIterations)
        km.run(awpairs.map((tup) => {val (_,darr) = tup ; darr}).cache)
    }
    
    def runClustering(args: Array[String]) = {
        val beforeWavelets = currentTime
        val aspairs = processActivities(args).cache
        val keep = getEnvValue("SLP_COEFFICIENT_KEEP_RATIO", KEEP.toString).toDouble
        
        val awpairs = transformWavelets(aspairs, keep)
        val afterWavelets = currentTime

        val waveletTime = afterWavelets - beforeWavelets
        Console.println(s"Wavelet transformation took $waveletTime ms")

        val beforeClustering = currentTime
        val model = findClusters(awpairs.map({case(ao,w,smps) => (ao,w)}).cache)
        val afterClustering = currentTime
        
        val clusterTime = afterClustering - beforeClustering
        Console.println(s"Cluster centroid optimization took $clusterTime ms")
        
        (aspairs, awpairs, model)
    }
            
     def main(args: Array[String]) = {
         val (aspairs, awpairs, model) = runClustering(args)
         
         val predictions = awpairs.map({case (activityAndOffset, coeffs, _s) => (model.predict(coeffs), activityAndOffset)}).sortByKey()
         val np_pairs = awpairs.map({case (activityAndOffset, _c, samples) => (activityAndOffset, NP.calculate(samples))}).collectAsMap()
         
         for (tup <- predictions.collect) {
             tup match {
                 case (ctr,ao @ (a,o)) => {
                     val hours = (o * OFFSET) / (60*60)
                     val minutes = ((o * OFFSET) / 60) - (hours*60)
                     val seconds = (o * OFFSET) % 60
                     val np = np_pairs.getOrElse(ao, -1.0)

                     Console.println("%s at offset %d:%02d:%02d (NP %f) is in cluster %d".format(a, hours, minutes, seconds, np, ctr))
                 }
             }
         }
         
         for ((activity, samples) <- aspairs) {
             val np = NP.calculate(samples)
             Console.println(s"NP for $activity is $np")
         }
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