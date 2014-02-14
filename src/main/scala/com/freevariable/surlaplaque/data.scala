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

case class ZoneBuckets(z1:Long, z2:Long, z3:Long, z4:Long, z5:Long, z6:Long, z7:Long) {
    def +(other: ZoneBuckets) = new ZoneBuckets(z1+other.z1, z2+other.z2, z3+other.z3, z4+other.z4, z5+other.z5, z6+other.z6, z7+other.z7)
}

object ZoneBuckets {
    def empty() = new ZoneBuckets(0,0,0,0,0,0,0)
}

case class ZoneHistogram(buckets: ZoneBuckets, recorder:((ZoneBuckets, Double, Long) => ZoneBuckets)) {
    def record(sample:Double, ct:Long = 1) = ZoneHistogram(recorder(buckets, sample, ct), recorder)
}

object ZoneHistogram {
    def make(ftp:Int) = {
        val recorder = ((b:ZoneBuckets, sample:Double, ct:Long) => sample match {
            case d:Double if d >= 0 && d <= ftp * 0.55 => new ZoneBuckets(b.z1+ct,b.z2,b.z3,b.z4,b.z5,b.z6,b.z7)
            case d:Double if d <= ftp * 0.75 => new ZoneBuckets(b.z1,b.z2+ct,b.z3,b.z4,b.z5,b.z6,b.z7)
            case d:Double if d <= ftp * 0.9 => new ZoneBuckets(b.z1,b.z2,b.z3+ct,b.z4,b.z5,b.z6,b.z7)
            case d:Double if d <= ftp * 1.05 => new ZoneBuckets(b.z1,b.z2,b.z3,b.z4+ct,b.z5,b.z6,b.z7)
            case d:Double if d <= ftp * 1.2 => new ZoneBuckets(b.z1,b.z2,b.z3,b.z4,b.z5+ct,b.z6,b.z7)
            case d:Double if d <= ftp * 1.5 => new ZoneBuckets(b.z1,b.z2,b.z3,b.z4,b.z5,b.z6+ct,b.z7)
            case d:Double if d > ftp * 1.5 => new ZoneBuckets(b.z1,b.z2,b.z3,b.z4,b.z5,b.z6,b.z7+ct)
            case _ => b
        }
        )
        new ZoneHistogram(ZoneBuckets.empty(), recorder)
    }
}

object Timestamp {
    def stringify(ts: Long) = ts.toDateTime.toString()
}

object Trackpoint {
    def apply(ts_string: String, latlong: Coordinates, altitude: Double, watts: Double, activity: Option[String] = None) = 
        new Trackpoint(ts_string.toDateTime.millis, latlong, altitude, watts, activity)
}
