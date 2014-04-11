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

sealed case class Polygon(points: List[Coordinates]) extends GeometryPrimitives {
  lazy val closedPoints =
    this.points.last::this.points

  lazy val isCW = 
    (closedPoints sliding 3).forall {case List(a:Coordinates, b:Coordinates, c:Coordinates) => clockwise(a,b,c)}

  lazy val isCCW =
    (closedPoints sliding 3).forall {case List(a:Coordinates, b:Coordinates, c:Coordinates) => !clockwise(a,b,c)}
  
    /**
      calculate whether or not p is in this polygon via the winding number method; adapted from http://geomalgorithms.com/a03-_inclusion.html
    */
  def includesPoint(p: Coordinates) = {
    def cccross(o:Coordinates,a:Coordinates,b:Coordinates) =
      ccross(csub(a,o), csub(b,o))
    
    def ipHelper(p: Coordinates, poly: List[List[Coordinates]], wn: Int): Int = poly match {
      case List(v1, v2)::ps => 
        if (v1.lat <= p.lat && v2.lat > p.lat && cccross(v1, v2, p) > 0) {
          ipHelper(p, ps, wn + 1)
        } else if (v2.lat <= p.lat && cccross(v1, v2, p) < 0) {
          ipHelper(p, ps, wn - 1)
        } else {
          ipHelper(p, ps, wn)
        }
      case Nil => wn
    }
    
    ipHelper(p, (closedPoints sliding 2).toList, 0) != 0
  }
  
  val length = points.length
}