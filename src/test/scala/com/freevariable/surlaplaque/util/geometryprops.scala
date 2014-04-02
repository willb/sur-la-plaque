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