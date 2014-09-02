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

object SLP {
    import java.io.File
    def listFilesInDir(dirname: String): List[String] = {
        val dir = new java.io.File(dirname)
	if (dir.exists && dir.isDirectory) {
          dir.listFiles.filter(_.isFile).toList.map(dirname + "/" + _.getName.toString).filter(fn => fn.endsWith(".tcx"))
	} else {
	  println(s"warning:  $dirname either does not exist or is not a directory")
	  Nil
	}
    }
    
    def expandArgs(args: Array[String]): Array[String] = {
        val Pattern = "^-d(.*)$".r
        args.toList.foldLeft(List():List[String])((ls, arg) =>
            arg match {
                case Pattern(dir) => ls ++ listFilesInDir(dir)
                case arg:String => arg::ls
            }
        ).toArray
    }
}

class SLP(sc: SparkContext) {
    import com.freevariable.surlaplaque.importer.extract
    import com.freevariable.surlaplaque.power.MMP
    
    def processFiles(files: Seq[String]) = 
        sc.parallelize(files, sc.defaultParallelism * 4).flatMap((s:String) => extract.trackpointDataFromFile(s))

    def processFiles(files: Seq[String], period: Int) = 
        sc.parallelize(files.flatMap((s:String) => MMP.calculate(extract.trackpointDataFromFile(s).toList, period)))
    
    def stop {
      sc.stop
    }
    
    def context = sc
}

trait Common {
    import java.io._
    
    private var exitHooks: List[() => Unit] = Nil
    
    def master = sys.env.get("SLP_MASTER") match {
        case Some(v) => v
        case None => "local[8]"
    }
    
    def appName = "sur-la-plaque"
    
    def ftp = sys.env.get("SLP_FTP") match {
        case Some(v) => v.toInt
        case None => 300
    }
    
    def outputFileName = sys.env.get("SLP_OUTPUT_FILE") match {
        case Some(filename) => filename
        case None => "slp.json"
    }
    
    def outputFile(f: String = "") = (if (f == "") outputFileName else f) match {
        case "--" => new PrintWriter(System.err)
        case filename => new PrintWriter(new File(filename))
    }
    
    def getEnvValue(variable:String, default:String) = sys.env.get(variable) match {
        case Some(v) => v
        case None => default
    }
    
    def main(args: Array[String]) = {
      appMain(args)
      runExitHooks
    }
    
    def addExitHook(thunk: => Unit) {
      exitHooks = {() => thunk} :: exitHooks
    }
    
    def runExitHooks() {
      for (hook <- exitHooks) {
        hook()
      }
    }
    
    def appMain(args: Array[String])
}

trait ActivitySliding {
  import org.apache.spark.rdd.RDD
  import com.freevariable.surlaplaque.data.Trackpoint
  
  def windowsForActivities[U](data: RDD[Trackpoint], period: Int, xform: (Trackpoint => U) = identity _) = {
    val pairs = data.groupBy((tp:Trackpoint) => tp.activity.getOrElse("UNKNOWN"))
    pairs.flatMap({case (activity:String, stp:Seq[Trackpoint]) => (stp sliding period).zipWithIndex.map {case (s,i) => ((activity, i), s.map(xform))}})
  }
  
  def identity(tp: Trackpoint) = tp
}

trait PointClustering {
  import org.apache.spark.rdd.RDD
  import org.apache.spark.mllib.clustering._
  import org.apache.spark.mllib.linalg.Vectors

  import com.freevariable.surlaplaque.data.Trackpoint
  
  import com.freevariable.surlaplaque.data.Coordinates
  
  def clusterPoints(rdd: RDD[Trackpoint], numClusters: Int, numIterations: Int) = {
    val km = new KMeans()
    km.setK(numClusters)
    km.setMaxIterations(numIterations)
    
    val vecs = rdd.map(tp => Vectors.dense(Array(tp.latlong.lon, tp.latlong.lat))).cache()
    km.run(vecs)
  }
  
  def closestCenter(tp: Trackpoint, model: KMeansModel) = model.predict(Vectors.dense(Array(tp.latlong.lon, tp.latlong.lat)))
  
  def closestCenter(coords: Coordinates, model: KMeansModel) = model.predict(Vectors.dense(Array(coords.lon, coords.lat)))
  
}
