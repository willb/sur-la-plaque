package com.freevariable.surlaplaque.data;

import com.github.nscala_time.time.Imports._

sealed case class Coordinates(lat: Double, lon: Double) {}
    
sealed case class Trackpoint(timestamp: Long, latlong: Coordinates, altitude: Double, watts: Double) {
    val timestring = timestamp.toDateTime.toString()
}

object Trackpoint {
    def apply(ts_string: String, latlong: Coordinates, altitude: Double, watts: Double) = 
        new Trackpoint(ts_string.toDateTime.millis, latlong, altitude, watts)
}
