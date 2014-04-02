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
    def mean(i: Iterator[Double]) = {
        val (tot, ct) = i.aggregate((0.0,0))({case ((t,c),smp) => (smp+t, c+1)}, {case ((t1,c1), (t2,c2)) => (t1+t2, c1+c2)})
        tot/ct
    }
    
    def calculate(data: Array[Double]) = {
        val rollingAverages = (data sliding 30).map(_.reduce (_+_) / 30.0)
        val weightedAverages = rollingAverages.map(math.pow(_, 4))
        math.pow(mean(weightedAverages), 1.0/4)
    }
}