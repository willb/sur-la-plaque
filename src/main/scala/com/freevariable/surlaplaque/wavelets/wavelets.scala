package com.freevariable.surlaplaque.wavelets


class WaveletExtractor(windowSize: Int, skip: Int, keepRatio: Double) extends Serializable {
    import breeze.linalg._
    import breeze.signal._

    import collection.SortedSet
    import math.{max,min}
    
    private def waveletize(samples: Array[Double]) = haarTr(DenseVector(samples)).toArray
    
    /* zeroes out the coefficients with the lowest magnitude; always keeps at least one */
    private def sparsify(samples:  Array[Double], keepRatio: Double) = {
        val keepCount = min(samples.length, max((keepRatio * samples.length).toInt, 1))
        val sorted = collection.SortedSet(samples.toArray :_*)
        val minMag = (sorted.takeRight(keepCount) | sorted.take(keepCount)).map(math.abs(_)).takeRight(keepCount).min
        
        samples.map((smp) => if (math.abs(smp) < minMag) 0.0 else smp)
    }

    def transformAndAbstract(samples: Array[Double]) = 
        for (window <- samples.sliding(windowSize, skip))
            yield (sparsify(waveletize(window), keepRatio), window)
}

object WaveletExtractor {
    def apply(windowSize: Int=1024, skip: Int=30, keepRatio: Double=0.15) = new WaveletExtractor(windowSize, skip, keepRatio)
}