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

object PowerBestsApp extends Common with ActivitySliding {
  import org.apache.spark.rdd.RDD
  import org.apache.spark.SparkConf
    
  import com.freevariable.surlaplaque.geometry.ConvexHull

  class PBOptions(periodColors: Map[Int, Triple[Byte,Byte,Byte]], clusters: Int, iterations: Int, fls: List[String]) {
    def periodMap = {
      if (periodColors.size == 0) 
        Map(getEnvValue("SLP_MMP_PERIOD", "60").toInt -> Triple(255,0,0))
      else 
        periodColors
    }
    
    val files = this.fls
    
    def withPeriodColoring(period: Int, r: Byte, g: Byte, b: Byte) =
      new PBOptions(this.periodColors + Pair(period, Triple(r,g,b)), this.clusters, this.iterations, this.files)
    
    def withClusters(clusters: Int) = new PBOptions(this.periodColors, clusters, this.iterations, this.files)

    def withIterations(iterations: Int) = new PBOptions(this.periodColors, this.clusters, iterations, this.files)
    
    def withFile(file: String) = new PBOptions(this.periodColors, this.clusters, this.iterations, file::this.files)

    def withFiles(fs: List[String]) = new PBOptions(this.periodColors, this.clusters, this.iterations, fs ++ this.files)
  }
  
  object PBOptions {
    val default = new PBOptions(Map(), getEnvValue("SLP_CLUSTERS", "128").toInt, getEnvValue("SLP_ITERATIONS", "10").toInt, List())
  }
  
  def parseArgs(args: Array[String]) = {
    val hexRGB = "^((?:[0-9a-fA-F]){2})((?:[0-9a-fA-F]){2})((?:[0-9a-fA-F]){2})$".r
    val dirPattern = "^-d(.*)$".r

    def phelper(params: List[String], options: PBOptions): PBOptions = {
      params match {
        case dirPattern(dir) :: rest => phelper(rest, options.withFiles(SLP.listFilesInDir(dir)))
        case "--activity-dir" :: dir :: rest => phelper(rest, options.withFiles(SLP.listFilesInDir(dir)))
        case "--period-coloring" :: period :: hexRGB(r,g,b) :: rest => {
          val rb = Integer.parseInt(r, 16).toByte
          val gb = Integer.parseInt(g, 16).toByte
          val bb = Integer.parseInt(b, 16).toByte
          phelper(rest, options.withPeriodColoring(period.toInt, rb, gb, bb))
        }
        case "--clusters" :: c :: rest => phelper(rest, options.withClusters(c.toInt))
        case "--iterations" :: it :: rest => phelper(rest, options.withIterations(it.toInt))
        case "--" :: rest => options.withFiles(rest)
        case bogusOpt if bogusOpt(0) == "-" => throw new RuntimeException(s"unrecognized option $bogusOpt")
        case file :: rest => phelper(rest, options.withFile(file))
        case Nil => options
      }
    }
    phelper(args.toList, PBOptions.default)
  }

  
  def main(args: Array[String]) {
    val struct = run(args)
    
    val out = outputFile
    
    // out.println(struct.toJson)
    
    out.close
  }
  
  // val mmps = windowsForActivities(data, mmpPeriod).map {case ((a,i),s) => {val ll = s.head.latlong; (Array(ll.lon, ll.lat), s.map(_.watts).reduce(_ + _) / s.size)}}
  
  def run(args: Array[String]) = {
    val conf = new SparkConf()
                 .setMaster(master)
                 .setAppName(appName)
                 .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    val app = new SLP(new SparkContext(conf))
    
    val options = parseArgs(args)
    
    val data = app.processFiles(options.files.toArray)
    
    val struct = Map("type"->"FeatureCollection", "features"->List())
    
    struct
  }
  
  def rgba(r: Byte, g: Byte, b: Byte, a: Byte) = s"rgba($r, $g, $b, $a)"
}
