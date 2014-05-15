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

sealed case class Polygon(points: List[Coordinates], properties: Map[String, String] = Map()) extends GeometryPrimitives {
  lazy val closedPoints =
    this.points ++ List(this.points.head)

  lazy val isCW = 
    (closedPoints sliding 3).forall {case List(a:Coordinates, b:Coordinates, c:Coordinates) => clockwise(a,b,c)}

  lazy val isCCW =
    (closedPoints sliding 3).forall {case List(a:Coordinates, b:Coordinates, c:Coordinates) => !clockwise(a,b,c)}
  
  lazy val pointSet =
    this.points.toSet
  
    /**
      calculate whether or not p is in this polygon via the winding number method; adapted from http://geomalgorithms.com/a03-_inclusion.html
    */
  def includesPoint(p: Coordinates) = {
    
    def windingNum(p: Coordinates, poly: List[List[Coordinates]], wn: Int): Int = poly match {
      case List(v1, v2)::ps => 
        if (v1.lon <= p.lon && v2.lon > p.lon && isLeft(v1, v2, p) > 0) {
          // Console.println(s"incrementing wn (was $wn)")
          windingNum(p, ps, wn + 1)
        } else if (!(v1.lon <= p.lon) && v2.lon <= p.lon && isLeft(v1, v2, p) < 0) {
          // Console.println(s"decrementing wn (was $wn)")
          windingNum(p, ps, wn - 1)
        } else {
          // Console.println(s"not changing wn (was $wn)")
          windingNum(p, ps, wn)
        }
      case Nil => wn
    }
    
    pointSet.contains(p) || windingNum(p, (closedPoints sliding 2).toList, 0) != 0
  }
  
  val length = points.length
  
  def annotate(k: String, v: String): Polygon =
    Polygon(this.points, this.properties + Pair(k, v))
  
}

object Polygon {
  import org.json4s._
  import org.json4s.JsonDSL._
  import org.json4s.jackson.JsonMethods._
  
  implicit def poly2json(p: Polygon): JValue = {
    ("type" -> "Feature") ~
    ("geometry" -> 
      ("type" -> "Polygon") ~
      ("coordinates" -> List(p.closedPoints.map {coords => List(coords.lon, coords.lat)}))) ~
    ("properties" -> p.properties)
  }
}