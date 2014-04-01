package com.freevariable.surlaplaque.data;

import com.github.nscala_time.time.Imports._

sealed case class Coordinates(lat: Double, lon: Double) extends Ordered[Coordinates] {
  import scala.math.Ordered.orderingToOrdered
  
  import com.freevariable.surlaplaque.util.RWDistance.{distance => rw_distance}
  
  /**
    Approximate distance between this and other in meters
  */
  def distance(other:Coordinates) = rw_distance((lat, lon), (other.lat, other.lon))

  /**
    Ordering based on longitude then latitude
  */
  def compare(other: Coordinates) = 
    (this.lon, this.lat) compare (other.lon, other.lat)
  
  /** 
    Ordering based on latitude (then longitude, if necessary) 
  */
  def compare_lat(other: Coordinates) = 
    (this.lat, this.lon) compare (other.lat, other.lon)
}

sealed case class Trackpoint(timestamp: Long, latlong: Coordinates, altitude: Double, watts: Double, activity: Option[String]) extends Ordered[Trackpoint] {
  import scala.math.Ordered.orderingToOrdered
  import Timestamp.{stringify => stringify_ts}
  
  val timestring = stringify_ts(timestamp)
    
  def elevDelta(other: Trackpoint) = other.altitude - altitude
  def timeDelta(other: Trackpoint) = (other.timestamp - timestamp).toDouble / 1000
  def distanceDelta(other: Trackpoint) = (other.latlong.distance(latlong))
  def kphBetween(other:Trackpoint) = ((other.latlong.distance(latlong)) / timeDelta(other)) * 3600
  def gradeBetween(other:Trackpoint) = {
    val rise = elevDelta(other) // rise is in meters
    val run = distanceDelta(other) * 10 // run is in km, but we want to get a percentage grade
    rise/run
  }
    
  def compare(other: Trackpoint) = (this.latlong.lon, this.latlong.lat, this.timestamp, this.altitude, this.watts) compare (other.latlong.lon, other.latlong.lat, other.timestamp, other.altitude, other.watts)
}

object Timestamp {
    def stringify(ts: Long) = ts.toDateTime.toString()
}

object Trackpoint {
    def apply(ts_string: String, latlong: Coordinates, altitude: Double, watts: Double, activity: Option[String] = None) = 
        new Trackpoint(ts_string.toDateTime.millis, latlong, altitude, watts, activity)
}
