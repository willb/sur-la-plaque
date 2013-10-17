package com.freevariable.surlaplaque.importer;

import scala.xml.XML

object extract {
    def tupleFromTrackpoint(tp: scala.xml.Node) = ( timestamp(tp), latlong(tp), alt(tp), watts(tp) )

    def timestamp(tp: scala.xml.Node) = (tp \ "Time").text

    def latlong(tp: scala.xml.Node) = {
        val lat = (tp \\ "LatitudeDegrees").text.toDouble
        val lon = (tp \\ "LongitudeDegrees").text.toDouble
        (lat, lon)
    }

    def alt(tp: scala.xml.Node) = (tp \ "AltitudeMeters").text.toDouble

    def watts(tp: scala.xml.Node) = (tp \\ "Watts").text match {
        case "" => 0.0
        case x: String => x.toDouble
    }
    
    def trackpointDataFromFile(tcx: String) = {
        val tcxTree = XML.loadFile(tcx)
        (tcxTree \\ "Trackpoint").map(extract.tupleFromTrackpoint(_))
    }
}

object TCX2CSV {
    def main(args: Array[String]) {
        for (file <- args.toList) 
            for ((timestamp, (lat, long), alt, watts) <- extract.trackpointDataFromFile(file))
                Console.println("%s,%f,%f,%f,%f".format(timestamp, lat, long, alt, watts))
    }
}