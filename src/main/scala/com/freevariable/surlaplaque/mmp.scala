package com.freevariable.surlaplaque.mmp;

import com.freevariable.surlaplaque.data._

case class MMPTrackpoint(tp: Trackpoint, period: Int, mmp: Double)

object MMP {
    def calculate(data: List[Trackpoint], period: Int) = {
        val wattages = List.fill(period - 1)(0.0d) ++ data.map(_.watts)
        val mmps = (wattages sliding period) map ((lastWatts:List[Double]) => lastWatts.sum / period)
        (data, mmps.toIterable).zipped.map(new MMPTrackpoint(_, period, _))
    }
}
