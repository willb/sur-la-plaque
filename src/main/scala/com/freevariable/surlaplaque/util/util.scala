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

object PowerQuantizer {
    def make_quantizer(floors: List[Double]) = {
        val sortedFloors = floors.sortWith(_ > _)
        ((v: Double) => sortedFloors.find(v >= _).getOrElse(0.0))
    }
}

case class StreamRecorder(count: Long, min: Double, max: Double, mean: Double, sumX2: Double) {
  import Math.sqrt

  def add(sample: Double) = {
    val dev = sample - mean
    val newCount = count + 1
    val newMean = mean + (dev / newCount)
    
    StreamRecorder(
      newCount, 
      if (sample < min) sample else min,
      if (sample > max) sample else max,
      newMean,
      sumX2 + (dev * (sample - newMean))
      )
  }
  
  def variance = sumX2 / count
  def stddev = sqrt(variance)
}

object StreamRecorder {
  import java.lang.{Double => JDouble}
  
  def apply() = empty
  def empty = new StreamRecorder(0, JDouble.MAX_VALUE, JDouble.MIN_VALUE, 0, 0)
}