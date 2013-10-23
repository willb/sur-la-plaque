package com.freevariable.surlaplaque;

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

import com.freevariable.surlaplaque.importer._

class SLP(sc: SparkContext) {
    def processFiles(files: Array[String], mmpPeriod: Int = 60) = {
        val tupleSets = files.map((_, extract.trackpointDataFromFile(_)))
        
    }
}

object Main {
    
    def main(args: Array[String]) {
        val appName = "sur-la-plaque"
        val master = sys.env.get("SLP_MASTER") match {
            case Some(v) => v
            case None => "local[2]"
        }
        
        // XXX: add optional parameters here to support cluster execution
        val app = new SLP(new SparkContext(master, appName))
        
        
    }
}