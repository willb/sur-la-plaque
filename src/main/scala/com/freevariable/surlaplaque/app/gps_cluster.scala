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

object GPSClusterApp extends Common with ActivitySliding with PointClustering {
  import spray.json._
  import DefaultJsonProtocol._
  import org.apache.spark.rdd.RDD
  import org.apache.spark.SparkConf
  import org.apache.spark.mllib.linalg.{Vector, Vectors}
  
  import com.freevariable.surlaplaque.geometry.ConvexHull
  
  def main(args: Array[String]) {
    val mmpPeriod = getEnvValue("SLP_MMP_PERIOD", "60").toInt
      
    val struct = run(args, mmpPeriod, makeMmpColorer _)
    
    val out = outputFile()
    
    out.println(struct.toJson)
    
    out.close
  }
  
  def run(args: Array[String], mmpPeriod: Int, makeColorer: ((RDD[Trackpoint], Int, KMeansModel) => (Int => String))) = {
    val conf = new SparkConf()
                 .setMaster(master)
                 .setAppName(appName)
                 .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    val app = new SLP(new SparkContext(conf))
    
    val data = app.processFiles(SLP.expandArgs(args))
    
    val numClusters = getEnvValue("SLP_CLUSTERS", "128").toInt
    val numIterations = getEnvValue("SLP_ITERATIONS", "10").toInt
    
    val model = clusterPoints(data, numClusters, numIterations)
    
    val hulls = generateHulls(data, model)

    val colorer = makeColorer(data, mmpPeriod, model)
    
    val hullPolys = hulls.map({case (cluster, poly) => makeHullPoly(cluster, poly.points, colorer)}).collect
    
    val struct = Map("type"->"FeatureCollection".toJson, "features"->hullPolys.toJson)
        
    struct
  }

  def makeMmpColorer(data: RDD[Trackpoint], mmpPeriod: Int, model: KMeansModel) = {
    val mmps = windowsForActivities(data, mmpPeriod).map {case ((a,i),s) => {val ll = s.head.latlong; (Vectors.dense(Array(ll.lon, ll.lat)), s.map(_.watts).reduce(_ + _) / s.size)}}
    
    val bestPerCluster = mmps
      .map({case (ll:Vector, mmp:Double) => {val cluster = model.predict(ll); (cluster, mmp)}})
      .reduceByKey((a:Double, b:Double) => if (a>b) a else b)
      .collectAsMap()
    
    val overallBestMMP = bestPerCluster.values.max
    val overallLeastMMP = bestPerCluster.values.min
    
    Console.println(s"worst/best $mmpPeriod second MMP: $overallLeastMMP, $overallBestMMP")
    Console.println(s"clusters->mmps $bestPerCluster")
    
    val colorer = ((cluster:Int) => {
      val intensity = ((bestPerCluster.getOrElse(cluster, overallLeastMMP) - overallLeastMMP) / (overallBestMMP - overallLeastMMP))
      val gb = (255 - (255 * intensity)).toByte
      rgb(255.toByte, gb, gb)
    })
    
    colorer
  }

  def makeDensityColorer(data: RDD[Trackpoint], mmpPeriod: Int, model: KMeansModel) = {
    // NB: "density" is a misnomer since this colors based on counts and not on count per unit of area
    // the latter will follow after some refactoring
    val clusterDensities = data.map((tp:Trackpoint) => (model.predict(Vectors.dense(Array(tp.latlong.lon, tp.latlong.lat))), 1))
      .reduceByKey(_ + _)
      .collectAsMap()
    
    val maxDensity = clusterDensities.values.max
    val minDensity = clusterDensities.values.min
    
    Console.println(s"min/max point counts: $minDensity, $maxDensity")
    Console.println(s"clusterDensities:  $clusterDensities")
    
    val colorer = ((cluster:Int) => {
      val intensity = ((clusterDensities(cluster) - minDensity) / (maxDensity - minDensity).toDouble)
      val cval = (255 - (255 * intensity)).toByte
      val c = rgb(cval, cval, cval)
      Console.println(s"color for cluster $cluster is $c (intensity is $intensity, raw count is "+clusterDensities(cluster).toString+")")
      c
    })
    
    colorer
  }
  
  def generateHulls(rdd: RDD[Trackpoint], model: KMeansModel) = {
    val clusteredPoints = rdd.groupBy((tp:Trackpoint) => closestCenter(tp, model))
    clusteredPoints.map({case (ctr, pts) => (ctr, ConvexHull.calculate(pts.map(_.latlong).toList))})
  }

  def makeHullPoly(cluster:Int, coords:List[Coordinates], coloring:(Int=>String) = ((x:Int) => rgb((x*2).toByte, 0, 0))) = {
    val acoords = coords.map(c => Array(c.lon, c.lat).toJson).toArray

    Map(
      "type" -> "Feature".toJson,
      "geometry" -> Map("type"->"Polygon".toJson, "coordinates"->Array((acoords ++ Array(acoords(0))).toJson).toJson).toJson,
      "properties" -> Map("fill"->coloring(cluster), "fill-opacity"->"0.8", "stroke"->"#aaaaaa"/*, "stroke-width"->"0"*/).toJson
    )
  }

  def makePointMap(cluster:Int, count:Long, coords:Array[Double], max:Long) = {
    val frac = count.toDouble / max
    val color = var_gb(frac)
    val ssize = symsize(frac)
    Map(
      "type" -> "Feature".toJson,
      "geometry" -> Map("type"->"Point".toJson, "coordinates"->Array(coords(1), coords(0)).toJson).toJson,
      "properties" -> Map("marker-color"->color/*, "marker-size"->ssize, "marker-symbol"->"circle"*/).toJson
    )
  }
  
  def symsize(frac:Double) = frac match {
    case x if x < 0.33 => "small"
    case x if x < 0.67 => "medium"
    case _ => "large"
  }
  
  def var_gb(frac:Double) = {
    val gb = ((1-frac) * 256).toInt
    "#ff%02x%02x".format(gb,gb)
  }
  
  def rgb(r: Byte, g: Byte, b: Byte) = "#%02x%02x%02x".format(r,g,b)
}
