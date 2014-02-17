package com.freevariable.surlaplaque.util

object PowerQuantizer {
    def make_quantizer(floors: List[Double]) = {
        val sortedFloors = floors.sortWith(_ > _)
        ((v: Double) => sortedFloors.find(v >= _).getOrElse(0.0))
    }
}

