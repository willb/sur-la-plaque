package com.freevariable.surlaplaque;

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

import com.freevariable.surlaplaque.importer._
import com.freevariable.surlaplaque.data._

class SLP(sc: SparkContext) {
    def processFiles(files: Array[String], mmpPeriod: Int = 60) = {
        val points = files.flatMap((s:String) => extract.trackpointDataFromFile(s))
        sc.parallelize(points)
    }
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

