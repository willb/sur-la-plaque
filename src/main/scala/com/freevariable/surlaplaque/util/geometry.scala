// not-particularly-robust geometrical primitives

package com.freevariable.surlaplaque.util

import com.freevariable.surlaplaque.data.{Coordinates, Trackpoint}

trait GeometryPrimitives {
  // TODO:  add some basic primitives here to enable pushing more of
  // convex hull creation out to RDD-land
}

object ConvexHull {
  def calculate(points: List[Coordinates]): List[Coordinates] = {
    val sortedPoints = points.sorted.toSet.toList
    
    if (sortedPoints.length <= 1) {
      sortedPoints
    } else {
      val _::lowerHull = buildHull(points, List())
      val _::upperHull = buildHull(points.reverse, List())
      lowerHull.reverse ++ upperHull.reverse
    }
  }
  
  private[this] def turnType(a:Coordinates, b:Coordinates, c:Coordinates) = {
    val Coordinates(ay,ax) = a
    val Coordinates(by,bx) = b
    val Coordinates(cy,cx) = c
    (ax*by) - (ay*bx) + (ay*cx) - (ax*cy) + (bx*cy) - (cx*by)
  }
  
  private[this] def buildHull(points: List[Coordinates], hull: List[Coordinates]): List[Coordinates] = {
    (points, hull) match {
      case (Nil, _) => hull
      case (p::ps, first::second::rest) => {
        if(turnType(second, first, p) <= 0) {
          buildHull(points, second::rest)
        } else {
          buildHull(ps, p::first::second::rest)
        }
      }
      case (p::ps, first::Nil) => buildHull(ps, p::first::Nil)
      case (p::ps, Nil) => buildHull(ps, p::Nil)
    }
  }
}