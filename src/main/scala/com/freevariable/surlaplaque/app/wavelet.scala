package com.freevariable.surlaplaque.app

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.clustering._

import com.freevariable.surlaplaque.data.Trackpoint

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
        val we = new WaveletExtractor(1024, OFFSET, keep)
        rdd.flatMap((pair) => {
            val (activity, samples) = pair
            for (((wavelet, sampleWindow),idx) <- (we.transformAndAbstract(samples).zipWithIndex)) yield ((activity,idx), wavelet, sampleWindow)
        })
    }
    
    def findMMPs(rdd: RDD[(String, Array[Double])], period: Int = 120) = {
      rdd.flatMap({case (activity, samples) => 
        val mmps = (samples sliding period).map(window => window.reduce(_+_) / period)
        mmps.zipWithIndex.map(tup => (activity, tup._1, tup._2))
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
         
         val calculate_np = NP.calculate _
         
         val predictions = awpairs.map({case (activityAndOffset, coeffs, _s) => (model.predict(coeffs), activityAndOffset)}).sortByKey()
         val np_pairs = awpairs.map({case (activityAndOffset, _c, samples) => (activityAndOffset, calculate_np(samples))}).collectAsMap()
         val kj_rdd = awpairs.map({case (aao,c,s) => (aao,s.reduce(_ + _) / 1000.0)}).cache
         val kj_pairs = kj_rdd.collectAsMap()
         val top_kjs = kj_rdd.map({case (aao,kj) => (kj, aao)}).sortByKey().collect()
         
         for (tup <- predictions.collect) {
             tup match {
                 case (ctr,ao @ (a,o)) => {
                     val hours = (o * OFFSET) / (60*60)
                     val minutes = ((o * OFFSET) / 60) - (hours*60)
                     val seconds = (o * OFFSET) % 60
                     val np = np_pairs.getOrElse(ao, -1.0)
                     val kj = kj_pairs.getOrElse(ao, -1.0)
                     Console.println("%s at offset %d:%02d:%02d (NP %f, kj %f) is in cluster %d".format(a, hours, minutes, seconds, np, kj, ctr))
                 }
             }
         }
         
         for (tup <- top_kjs) {
           val (kj, aao) = tup
           val (a,o) = aao
           val hours = (o * OFFSET) / (60*60)
           val minutes = ((o * OFFSET) / 60) - (hours*60)
           val seconds = (o * OFFSET) % 60

           Console.println("%s at offset %d:%02d:%02d was %f kj".format(a, hours, minutes, seconds, kj))
         }
         
         for ((activity, samples) <- aspairs) {
             val np = NP.calculate(samples)
             val kj = samples.reduce(_ + _) / 1000
             Console.println(s"NP for $activity is $np ($kj kj)")
         }
     }
}