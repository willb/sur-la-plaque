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
}
