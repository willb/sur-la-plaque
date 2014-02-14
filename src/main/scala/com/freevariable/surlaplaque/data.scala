package com.freevariable.surlaplaque.data;

import com.github.nscala_time.time.Imports._

sealed case class Coordinates(lat: Double, lon: Double) {
    import com.freevariable.surlaplaque.util.RWDistance.{distance => rw_distance}
    
    def distance(other:Coordinates) = rw_distance((lat, lon), (other.lat, other.lon))
}

sealed case class Trackpoint(timestamp: Long, latlong: Coordinates, altitude: Double, watts: Double, activity: Option[String]) {
    val timestring = Timestamp.stringify(timestamp)
    
    def elevDelta(other: Trackpoint) = other.altitude - altitude
    def timeDelta(other: Trackpoint) = (other.timestamp - timestamp).toDouble / 1000
    def distanceDelta(other: Trackpoint) = (other.latlong.distance(latlong))
    def kphBetween(other:Trackpoint) = ((other.latlong.distance(latlong)) / timeDelta(other)) * 3600
    def gradeBetween(other:Trackpoint) = {
        val rise = elevDelta(other) // rise is in meters
        val run = distanceDelta(other) * 10 // run is in km, but we want to get a percentage grade
        rise/run
    }
}

object ZoneHistogram {
    def make(ftp:Int) = {
        case class Buckets(z1:Long, z2:Long, z3:Long, z4:Long, z5:Long, z6:Long, z7:Long) {
            def record(sample:Double, ct:Long = 1) = sample match {
                case d:Double if d <= ftp * 0.55 => new Buckets(z1+ct,z2,z3,z4,z5,z6,z7)
                case d:Double if d <= ftp * 0.75 => new Buckets(z1,z2+ct,z3,z4,z5,z6,z7)
                case d:Double if d <= ftp * 0.9 => new Buckets(z1,z2,z3+ct,z4,z5,z6,z7)
                case d:Double if d <= ftp * 1.05 => new Buckets(z1,z2,z3,z4+ct,z5,z6,z7)
                case d:Double if d <= ftp * 1.2 => new Buckets(z1,z2,z3,z4,z5+ct,z6,z7)
                case d:Double if d <= ftp * 1.5 => new Buckets(z1,z2,z3,z4,z5,z6+ct,z7)
                case d:Double if d > ftp * 1.5 => new Buckets(z1,z2,z3,z4,z5,z6,z7+ct)
                case _ => this
            }
                        
            def +(other: Buckets) = new Buckets(z1+other.z1, z2+other.z2, z3+other.z3, z4+other.z4, z5+other.z5, z6+other.z6, z7+other.z7)
        }
        new Buckets(0,0,0,0,0,0,0)
    }
}

object Timestamp {
    def stringify(ts: Long) = ts.toDateTime.toString()
}

object Trackpoint {
    def apply(ts_string: String, latlong: Coordinates, altitude: Double, watts: Double, activity: Option[String] = None) = 
        new Trackpoint(ts_string.toDateTime.millis, latlong, altitude, watts, activity)
}
