/*
 * This file is a part of the "sur la plaque" toolkit for cycling
 * data analytics and visualization.
 *
 * Copyright (c) 2013--2014 William C. Benton and Red Hat, Inc.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.freevariable.surlaplaque.app;

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

import org.apache.spark.mllib.clustering._

import com.freevariable.surlaplaque.importer._
import com.freevariable.surlaplaque.data._
// import com.freevariable.surlaplaque.power._
import com.freevariable.surlaplaque.app._

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