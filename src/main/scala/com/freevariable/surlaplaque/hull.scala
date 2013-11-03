
package com.freevariable.surlaplaque.quickhull;

import com.freevariable.surlaplaque.data._

import org.apache.spark.rdd._

object QuickHull {
  def terminalLongitudes(r:RDD[Coordinates]) = {
    val longitudes = r.map({case Coordinates(_,lon) => lon})
    val min = longitudes.reduce((f,s) => if (f < s) f else s)
    val max = longitudes.reduce((f,s) => if (f > s) f else s)
    (min, max)
  }

  // twice the area of a triangle defined by three points
  def triDet(a:Coordinates, b:Coordinates, c:Coordinates) = {
    val Coordinates(ay,ax) = a
    val Coordinates(by,bx) = b
    val Coordinates(cy,cx) = c
    (ax*by) - (ay*bx) + (ay*cx) - (ax*cy) + (bx*cy) - (cx*by)
  }
  
  sealed abstract class Position {}
  case class Above extends Position {}
  case class Below extends Position {}
  case class Upon extends Position {}
  
  def relativePosition(point:Coordinates, line_start:Coordinates, line_end:Coordinates) = {
      triDet(line_start, line_end, point) match {
          case 0 => Upon()
          case x if x > 0 => Above()
          case x if x < 0 => Below()
      }
  }
}
