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

import org.apache.spark.broadcast.Broadcast

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

  case class PBOptions(periodColors: Map[Int, Triple[Short,Short,Short]], val clusters: Int, val iterations: Int, val files: List[String], val defaultOpacity: Short, val outputFile: String, val httpEndpoint: Option[String]) {
    def periodMap = {
      if (periodColors.size == 0) 
        Map(getEnvValue("SLP_MMP_PERIOD", "60").toInt -> Triple[Short,Short,Short](255,0,0))
      else 
        periodColors
    }
    
    def withPeriodColoring(period: Int, r: Short, g: Short, b: Short) = this.copy(periodColors = this.periodColors + Pair(period, Triple[Short,Short,Short](r,g,b)))
    
    def withClusters(clusters: Int) = this.copy(clusters = clusters)

    def withIterations(iterations: Int) = this.copy(iterations = iterations)
    
    def withFile(file: String) = this.copy(files = file::this.files)

    def withFiles(fs: List[String]) = this.copy(files = fs ++ this.files) 
    
    def withDefaultOpacity(op: Short) = this.copy(defaultOpacity = op)
    
    def withOutputFile(f: String) = this.copy(outputFile = f)
    
    def withEndpoint(url: String) = this.copy(httpEndpoint = Some(url))
  }
  
  object PBOptions {
    val default = new PBOptions(Map(), getEnvValue("SLP_CLUSTERS", "256").toInt, getEnvValue("SLP_ITERATIONS", "10").toInt, List(), 128, getEnvValue("SLP_OUTPUT_FILE", "slp.json"), None)
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
        case "--url" :: url :: rest => phelper(rest, options.withEndpoint(url))
        case "--" :: rest => options.withFiles(rest)
        case bogusOpt if bogusOpt(0) == "-" => throw new RuntimeException(s"unrecognized option $bogusOpt")
        case file :: rest => phelper(rest, options.withFile(file))
      }
    }
    phelper(args.toList, PBOptions.default)
  }

  
  def appMain(args: Array[String]) {
    val options = parseArgs(args)
    val struct = run(options)
    
    val out = outputFile(options.outputFile)
    
    val renderedStruct = pretty(render(struct))
    
    out.println(renderedStruct)
    
    maybePut(options, renderedStruct)
    
    out.close
  }
  
  def run(options: PBOptions) = {
    val conf = new SparkConf()
                 .setMaster(master)
                 .setAppName(appName)
                 .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    val app = new SLP(new SparkContext(conf))
    addExitHook(app.stop)
    
    val data = app.processFiles(options.files)
    
    val bests = bestsByEndpointClusters(options, data, app)
        
    val struct = ("type"->"FeatureCollection") ~ ("features"->bests)
    
    struct
  }
  
  import Math.abs
  
  type AO = Pair[String, Int]
  type AOWatts = Pair[AO, Double]
  
  case class BasicTrackpoint(latlong: Coordinates, watts: Double) {}
  
  def stripTrackpoints(tp: Trackpoint) = BasicTrackpoint(tp.latlong, tp.watts)
  
  def bestsByEndpointClusters(options: PBOptions, data: RDD[Trackpoint], app: SLP) = {
    val model = app.context.broadcast(clusterPoints(data, options.clusters, options.iterations))
    def bestsForPeriod(data: RDD[Trackpoint], period: Int, app: SLP, model: Broadcast[KMeansModel]) = {
      val windowedSamples = windowsForActivities(data, period, stripTrackpoints _)

      val clusterPairs = windowedSamples
        .map {case ((activity, offset), samples) => ((activity, offset), (closestCenter(samples.head.latlong, model.value), closestCenter(samples.last.latlong, model.value)))}
      val mmps = windowedSamples.map {case ((activity, offset), samples) => ((activity, offset), samples.map(_.watts).reduce(_ + _) / samples.size)}

      val top20 = mmps.join(clusterPairs)
       .map {case ((activity, offset), (watts, (headCluster, tailCluster))) => ((headCluster, tailCluster), ((activity, offset), watts))}
       .reduceByKey ((a, b) => if (a._2 > b._2) a else b)
       .map { case ((_, _), keep) => keep }
       .takeOrdered(20)(Ordering.by[((String, Int), Double), Double] { case ((_, _), watts) => -watts})
        
      app.context.parallelize(top20, app.context.defaultParallelism * 4)
       .join (windowedSamples)
       .map {case ((activity, offset), (watts, samples)) => (watts, samples)}
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
  
  def maybePut(options: PBOptions, document: String) {
    import dispatch._
    import scala.concurrent.ExecutionContext.Implicits.global
    
    options.httpEndpoint match {
      case Some(endpoint) => {
        val request = url(endpoint).PUT
          .setBody(document)
          .addHeader("Content-type", "application/json")
        for (result <- Http(request OK as.String)) yield result
      }
      case None => {}
    }
  }
}
