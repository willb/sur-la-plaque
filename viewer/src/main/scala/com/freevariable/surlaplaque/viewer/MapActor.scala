package com.freevariable.surlaplaque.viewer

import akka.actor.{ActorRef, Actor, ActorSystem}

case object EntryNotFound

sealed abstract class CacheCommand
case class GetCommand(val k: String) extends CacheCommand
case class PutCommand(val k: String, val v: String) extends CacheCommand

sealed abstract class Document(val v: Any)
case class GenericDocument(doc: Any) extends Document(doc)
case class GeoDocument(doc: Any) extends Document(doc)
case class ScatterPlotDocument(doc: Any) extends Document(doc)

class DocumentCache extends Actor {
  private var cache = Map[String, String]("example" -> "foo")
  
  def receive = {
    case PutCommand(k:String, v:String) => {
      cache = cache + Pair(k, v)
    }
    case GetCommand(k:String) => sender ! cache.getOrElse(k, EntryNotFound).toString
    case x => sender ! s"$x IS NOT RECOGNIZED BUT THE DUDE ABIDES"
  }
}