package com.freevariable.surlaplaque.app;

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

import org.apache.spark.mllib.clustering._

import com.freevariable.surlaplaque.importer._
import com.freevariable.surlaplaque.data._
// import com.freevariable.surlaplaque.power._
import com.freevariable.surlaplaque.app._

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