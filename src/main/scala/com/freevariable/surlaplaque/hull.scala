package com.freevariable.surlaplaque.quickhull;

import com.freevariable.surlaplaque.data._

import org.apache.spark.rdd._

object Geometry {
    def terminalLongitudes(r:RDD[Coordinates]) = {
      val longitudes = r.map({case Coordinates(_,lon) => lon})
      val min = longitudes.reduce((f,s) => if (f < s) f else s)
      val max = longitudes.reduce((f,s) => if (f > s) f else s)
      (min, max)
    }

    // twice the area of a triangle defined by three points
    def triDet(a:Coordinates, b:Coordinates, c:Coordinates) = {
      val Coordinates(ax,ay) = a
      val Coordinates(bx,by) = b
      val Coordinates(cx,cy) = c
      (ax*by) - (ay*bx) + (ay*cx) - (ax*cy) + (bx*cy) - (cx*by)
    }
  
    sealed abstract class Position {}
    case class Above extends Position {}
    case class Below extends Position {}
    case class Upon extends Position {}
  
    def relativePosition(point:Coordinates, line_start:Coordinates, line_end:Coordinates) = {
        triDet(point, line_start, line_end) match {
            case 0 => Upon()
            case x if x > 0 => Above()
            case x if x < 0 => Below()
        }
    }
  
    // XXX
    def inPoly(point:Coordinates, poly:List[Coordinates]) = false
  
    def makeEnclosedFilter(poly:List[Coordinates]) = {
        new PartialFunction[Coordinates, Coordinates] {
            def apply(p: Coordinates) = p
            def isDefinedAt(p: Coordinates) = !inPoly(p, poly)
        }
    }
  
    def makeDistanceFinder(line_start:Coordinates, line_end:Coordinates) = {
        val latDelta = line_end.lat - line_start.lat
        val lonDelta = line_end.lon - line_start.lon
        val Coordinates(lat1,lon1) = line_start
        val Coordinates(lat2,lon2) = line_end
      
        val denom = (latDelta*latDelta) + (lonDelta*lonDelta)
      
        ((pt:Coordinates) => {
            val Coordinates(pt_lat,pt_lon) = pt
            val cp = (pt_lon - lon1) * lonDelta + (pt_lat - lat1) * latDelta match {
                case u if u < 0 => line_start
                case u if u > 1 => line_end
                case u => Coordinates(lat1 + u * latDelta, lon1 + u * lonDelta)
            }
            pt.distance(cp)
        })
    }

}

object QuickHull {
}
