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
import com.freevariable.surlaplaque.app._

object PowerBestsApp extends Common with ActivitySliding with PointClustering {
  import org.json4s._
  import org.json4s.JsonDSL._
  import org.json4s.jackson.JsonMethods._

  import org.apache.spark.rdd.RDD
  import org.apache.spark.SparkConf
    
  import com.freevariable.surlaplaque.geometry.LineString
  import com.freevariable.surlaplaque.geometry.LineString._  

  import scala.collection.immutable.TreeSet

  class PBOptions(periodColors: Map[Int, Triple[Short,Short,Short]], val clusters: Int, val iterations: Int, val files: List[String], val defaultOpacity: Short, val outputFile: String) {
    def periodMap = {
      if (periodColors.size == 0) 
        Map(getEnvValue("SLP_MMP_PERIOD", "60").toInt -> Triple[Short,Short,Short](255,0,0))
      else 
        periodColors
    }
    
    def withPeriodColoring(period: Int, r: Short, g: Short, b: Short) =
      new PBOptions(this.periodColors + Pair(period, Triple[Short,Short,Short](r,g,b)), this.clusters, this.iterations, this.files, this.defaultOpacity, this.outputFile)
    
    def withClusters(clusters: Int) = new PBOptions(this.periodColors, clusters, this.iterations, this.files, this.defaultOpacity, this.outputFile)

    def withIterations(iterations: Int) = new PBOptions(this.periodColors, this.clusters, iterations, this.files, this.defaultOpacity, this.outputFile)
    
    def withFile(file: String) = new PBOptions(this.periodColors, this.clusters, this.iterations, file::this.files, this.defaultOpacity, this.outputFile)

    def withFiles(fs: List[String]) = new PBOptions(this.periodColors, this.clusters, this.iterations, fs ++ this.files, this.defaultOpacity, this.outputFile)
    
    def withDefaultOpacity(op: Short) = new PBOptions(this.periodColors, this.clusters, this.iterations, this.files, op, this.outputFile)
    
    def withOutputFile(f: String) = new PBOptions(this.periodColors, this.clusters, this.iterations, this.files, this.defaultOpacity, f)
    
  }
  
  object PBOptions {
    val default = new PBOptions(Map(), getEnvValue("SLP_CLUSTERS", "256").toInt, getEnvValue("SLP_ITERATIONS", "10").toInt, List(), 128, getEnvValue("SLP_OUTPUT_FILE", "slp.json"))
  }
  
  def parseArgs(args: Array[String]) = {
    val hexRGB = "^((?:[0-9a-fA-F]){2})((?:[0-9a-fA-F]){2})((?:[0-9a-fA-F]){2})$".r
    val dirPattern = "^-d(.*)$".r

    def phelper(params: List[String], options: PBOptions): PBOptions = {
      params match {
        case Nil => options
        case dirPattern(dir) :: rest => phelper(rest, options.withFiles(SLP.listFilesInDir(dir)))
        case "--activity-dir" :: dir :: rest => phelper(rest, options.withFiles(SLP.listFilesInDir(dir)))
        case "--period-coloring" :: period :: hexRGB(r,g,b) :: rest => {
          val rb = Integer.parseInt(r, 16) % 256
          val gb = Integer.parseInt(g, 16) % 256 
          val bb = Integer.parseInt(b, 16) % 256
          phelper(rest, options.withPeriodColoring(period.toInt, rb.toShort, gb.toShort, bb.toShort))
        }
        case "--clusters" :: c :: rest => phelper(rest, options.withClusters(c.toInt))
        case "--iterations" :: it :: rest => phelper(rest, options.withIterations(it.toInt))
        case "--opacity" :: op :: rest => phelper(rest, options.withDefaultOpacity(op.toShort))
        case "--output-file" :: f :: rest => phelper(rest, options.withOutputFile(f))
        case "--" :: rest => options.withFiles(rest)
        case bogusOpt if bogusOpt(0) == "-" => throw new RuntimeException(s"unrecognized option $bogusOpt")
        case file :: rest => phelper(rest, options.withFile(file))
      }
    }
    phelper(args.toList, PBOptions.default)
  }

  
  def main(args: Array[String]) {
    val options = parseArgs(args)
    val struct = run(options)
    
    val out = outputFile(options.outputFile)
    
    out.println(pretty(render(struct)))
    
    out.close
  }
  
  def run(options: PBOptions) = {
    val conf = new SparkConf()
                 .setMaster(master)
                 .setAppName(appName)
                 .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    val app = new SLP(new SparkContext(conf))
    
    val data = app.processFiles(options.files)
    
    val bests = bestsByEndpointClusters(options, data, app)
        
    val struct = ("type"->"FeatureCollection") ~ ("features"->bests)
    
    struct
  }
  
  import Math.abs
  
  type AO = Pair[String, Int]
  type AOWatts = Pair[AO, Double]
  
  def bestsByEndpointClusters(options: PBOptions, data: RDD[Trackpoint], app: SLP) = {
    val model = clusterPoints(data, options.clusters, options.iterations)
    def bestsForPeriod(data: RDD[Trackpoint], period: Int, app: SLP, model: KMeansModel) = {
      val windowedSamples = windowsForActivities(data, period).cache
      val clusterPairs = windowedSamples
        .map {case ((activity, offset), samples) => ((activity, offset), (closestCenter(samples.head, model), closestCenter(samples.last, model)))}
      val mmps = windowedSamples.map {case ((activity, offset), samples) => ((activity, offset), samples.map(_.watts).reduce(_ + _) / samples.size)}

      val top20 = mmps.join(clusterPairs)
       .map {case ((activity, offset), (watts, (headCluster, tailCluster))) => ((headCluster, tailCluster), (watts, (activity, offset)))}
       .reduceByKey ((a, b) => if (a._1 > b._1) a else b)
       .map {case ((headCluster, tailCluster), (watts, (activity, offset))) => (watts, (activity, offset))}
       .sortByKey(false)
       .take(20)
        
      app.context.parallelize(top20)
       .map {case (watts, (activity, offset)) => ((activity, offset), watts)} 
       .join (windowedSamples)
       .map {case ((activity, offset), (watts, samples)) => (watts, samples)}
       .sortByKey(false)
       .collect
        
    }
    
    options.periodMap.flatMap { case(period: Int, color: Triple[Short,Short,Short]) =>
      val bests = bestsForPeriod(data, period, app, model)
      val best = bests.head._1
      bests.map {case (watts, samples) => LineString(samples.map(_.latlong), Map("stroke" -> rgba(color._1, color._2, color._3, (options.defaultOpacity * (watts / best)).toShort), "stroke-width" -> "7", "label" -> s"$watts watts"))}
    }
  }
  
  def bestsWithoutTemporalOverlap(options: PBOptions, data: RDD[Trackpoint], app: SLP) = {
    def bestsForPeriod(data: RDD[Trackpoint], period: Int, app: SLP) = {
      val windowedSamples = windowsForActivities(data, period).cache
      val mmps = windowedSamples.map {case ((activity, offset), samples) => (samples.map(_.watts).reduce(_ + _) / samples.size, (activity, offset))}
      val sorted = mmps.sortByKey(false).map {case (watts, (activity, offset)) => ((activity, offset), watts)}.take(1000) // FIXME: KLUDGE

      val trimmed = topWithoutOverlaps(period, 20, sorted.toList)

      val top20 = app.context.parallelize(trimmed).cache

      top20.join(windowedSamples).map {case ((activity, offset), (watts, samples)) => (watts, samples)}
    }
  
    def topWithoutOverlaps(period: Int, count: Int, candidates: List[AOWatts]) = {
      def thelper(activityPeriods: TreeSet[AO], 
        kept: List[AOWatts], 
        cs: List[AOWatts]): List[AOWatts] = {
      if (kept.length == count) {
        kept
      } else {
        cs match {
          case Nil => kept
          case first @ Pair(Pair(activity, offset), watts) :: rest => 
            if (activityPeriods.filter({case (a,o) => a == activity && abs(o - offset) < period}).size == 0) {
              thelper(activityPeriods + Pair(activity, offset), Pair(Pair(activity, offset), watts)::kept, rest)
            } else {
              thelper(activityPeriods, kept, rest)
            }
          }
        }
      }
    
      thelper(TreeSet[AO](), List[AOWatts](), candidates).reverse
    }
    
    options.periodMap.flatMap { case(period: Int, color: Triple[Short,Short,Short]) =>
      bestsForPeriod(data, period, app).collect.map {case (watts, samples) => LineString(samples.map(_.latlong), Map("color" -> rgba(color._1, color._2, color._3, 128), "label" -> s"$watts watts"))}
    }
  }
  
  def rgba(r: Short, g: Short, b: Short, a: Short) = s"rgba($r, $g, $b, $a)"
}
