package com.freevariable.surlaplaque.util

import com.freevariable.surlaplaque.data.Coordinates

import org.scalacheck.Properties
import org.scalacheck.Prop.forAll

import org.scalacheck._

import org.scalacheck.Gen
import org.scalacheck.Gen._

import org.scalacheck.Arbitrary
import org.scalacheck.Arbitrary._

object HullSpecification extends Properties("ConvexHull") with GeometryPrimitives {
  implicit def genCoords(): Gen[Coordinates] = for {
    lat <- Gen.choose(42.827055, 43.196613)
    lon <- Gen.choose(-90.220861, -89.44301)
  } yield Coordinates(lat, lon)

  implicit lazy val arbCoordList = Arbitrary { Gen.listOf(genCoords) }

  property("hullPointsAreUnique") = forAll { (points: List[Coordinates]) =>
    val hull = ConvexHull.calculate(points)
    hull.length == hull.toSet.size
  }

  property("outputPointsSubsetInputPoints") = forAll { (points: List[Coordinates]) =>
    val hull = ConvexHull.calculate(points)
    hull.toSet.diff(points.toSet).size == 0
  }

  property("doesntIncreasePointCount") = forAll { (points: List[Coordinates]) =>
    val hull = ConvexHull.calculate(points)
    hull.length <= points.length
  }
  
  property("hullIsConvex") = forAll { (points: List[Coordinates]) =>
    val hull = ConvexHull.calculate(points)
    if (hull.length > 3) {
      val hullPoly = hull ++ List(hull.head)
      (hullPoly sliding 3).forall {case List(a, b, c) => !clockwise(a,b,c)} 
    } else {
      true
    }
  }
}