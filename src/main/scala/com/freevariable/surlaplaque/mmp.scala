package com.freevariable.surlaplaque.power;

import com.freevariable.surlaplaque.data._

case class MMPTrackpoint(timestamp: Long, latlong: Coordinates, altitude: Double, immedWatts: Double, period: Int, mmp: Double) {
    def timestring = Timestamp.stringify(timestamp)
}

object MMPTrackpoint {
    def apply(tp: Trackpoint, period: Int, mmp: Double) = {
        new MMPTrackpoint(tp.timestamp, tp.latlong, tp.altitude, tp.watts, period, mmp)
    }
}

object MMP {
    // XXX: use RDD.sliding once it is available in a Spark release
    def calculate(data: List[Trackpoint], period: Int) = {
        val wattages = List.fill(period - 1)(0.0d) ++ data.map(_.watts)
        val mmps = (wattages sliding period) map ((lastWatts:List[Double]) => lastWatts.sum / period)
        (data, mmps.toIterable).zipped.map(MMPTrackpoint(_, period, _))
    }
}

object NP {
    def calculate(data: Array[Double]) = 
        math.pow((data sliding 30).map(_.reduce(_+_) / 30.0).map(math.pow(_, 4)).reduce(_+_), 1.0/4)
}