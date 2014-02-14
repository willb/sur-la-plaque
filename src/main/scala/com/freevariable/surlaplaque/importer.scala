package com.freevariable.surlaplaque.importer;

import com.freevariable.surlaplaque.data._

import scala.xml.XML

import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._

object extract {
    
    def tupleFromTrackpoint(tp: scala.xml.Node) = Trackpoint(timestamp(tp), latlong(tp), alt(tp), watts(tp) )

    def timestamp(tp: scala.xml.Node) = (tp \ "Time").text

    def latlong(tp: scala.xml.Node) = {
        val lat = (tp \\ "LatitudeDegrees").text.toDouble
        val lon = (tp \\ "LongitudeDegrees").text.toDouble
        new Coordinates(lat, lon)
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
            for (tp @ Trackpoint(timestamp, Coordinates(lat, long), alt, watts, Some(file)) <- extract.trackpointDataFromFile(file))
                Console.println("%s,%f,%f,%f,%f".format(tp.timestring, lat, long, alt, watts))
    }
}

object TCX2Json {
   import java.io._
   import com.freevariable.surlaplaque.SLP.expandArgs
   
   def outputFile = sys.env.get("TCX2J_OUTPUT_FILE") match {
       case Some("--") => new PrintWriter(System.err)
       case Some(filename) => new PrintWriter(new File(filename))
       case None => new PrintWriter(new File("slp.json"))
   }
   
    def main(args: Array[String]) {
       val processedArgs = expandArgs(args)
       
       val tuples = processedArgs.toList.flatMap((file => 
          for (tp @ Trackpoint(timestamp, Coordinates(lat, lon), alt, watts, Some(file)) <- extract.trackpointDataFromFile(file))
             yield ("timestamp" -> tp.timestring) ~ ("lat" -> lat) ~ ("lon" -> lon) ~ ("alt" -> alt) ~ ("watts" -> watts)
             )
             )
       val out = outputFile
       out.println(pretty(tuples))
       out.close
    }
}
