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

package com.freevariable.surlaplaque.geometry

import com.freevariable.surlaplaque.data.Coordinates

import scala.language.implicitConversions

sealed case class LineString(points: List[Coordinates], properties: Map[String, String] = Map()) extends Annotatable[LineString] {
  lazy val pointSet =
    this.points.toSet
  
  val length = points.length
  
  def annotate(k: AnnotationKey, v: AnnotationValue): LineString =
    LineString(this.points, this.properties + Pair(k, v))
  
  def decimate(factor: Int) = {
    def dhelper(pts: List[Coordinates], count: Int): List[Coordinates] = 
      (pts, count) match {
        case (Nil, _) => Nil
        case (hd::tl, _) if (count % factor == 0) => hd::dhelper(tl, count + 1)
        case (_::tl, _) => dhelper(tl, count + 1)
      }
    
    val newPoints = dhelper(this.points, 0)
    
    LineString(newPoints, this.properties)
  }
}

object LineString {
  import org.json4s._
  import org.json4s.JsonDSL._
  import org.json4s.jackson.JsonMethods._
  
  implicit def poly2json(p: LineString): JValue = {
    ("type" -> "Feature") ~
    ("geometry" -> 
      ("type" -> "LineString") ~
      ("coordinates" -> p.points.map {coords => List(coords.lon, coords.lat)})) ~
    ("properties" -> p.properties)
  }
}
