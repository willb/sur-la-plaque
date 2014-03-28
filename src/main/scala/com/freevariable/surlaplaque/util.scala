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
  def empty = new StreamRecorder(0, JDouble.MAX_VALUE, JDouble.MIN_VALUE, 0, 0)
}