package com.freevariable.surlaplaque.app

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

import com.freevariable.surlaplaque.data.Trackpoint

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
