package com.freevariable.surlaplaque.wavelets



object WaveletExtractor {
    import breeze.linalg._
    import breeze.signal._

    import collection.SortedSet
    
    private def waveletize(samples: Array[Double]) = haarTr(DenseVector(samples)).toArray
    
    private def tossQuietest(samples:  Array[Double], keepRatio: Double) = {
        val keepCount = (keepRatio * samples.length).toInt
        val sorted = collection.SortedSet(samples.toArray :_*)
        val minMag = (sorted.takeRight(keepCount) | sorted.take(keepCount)).map(math.abs(_)).takeRight(keepCount).min
        
        samples.map((smp) => if (math.abs(smp) < minMag) 0.0 else smp)
    }

    def extractWavelets(samples: Array[Double], windowSize: Int = 1024, skip: Int = 30, keepRatio: Double = 0.15) = samples.sliding(windowSize, skip).toList.par.map((darr) => tossQuietest(waveletize(darr), keepRatio))
}