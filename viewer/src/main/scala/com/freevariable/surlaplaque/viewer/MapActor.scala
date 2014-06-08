package com.freevariable.surlaplaque.viewer

import akka.actor.{ActorRef, Actor, ActorSystem}

case object EntryNotFound

sealed abstract class CacheCommand
case class GetCommand(val k: String) extends CacheCommand
case class PutCommand(val k: String, val v: Document) extends CacheCommand

sealed abstract class Document(val v: Any)
case class GenericDocument(doc: Any) extends Document(doc)
case class GeoDocument(doc: Any) extends Document(doc)
case class ScatterPlotDocument(doc: Any) extends Document(doc)
case object MissingDocument extends Document(Nil)

class DocumentCache extends Actor {
  private var cache = Map[String, Document]("example" -> GenericDocument("foo"))
  
  def receive = {
    case PutCommand(k:String, v:Document) => {
      cache = cache + Pair(k, v)
    }
    case GetCommand(k:String) => {
      val cached = cache.get(k)
      sender ! (cached match {
        case Some(doc) => doc
        case None => MissingDocument
      })
    }
    case x => sender ! s"$x IS NOT RECOGNIZED BUT THE DUDE ABIDES"
  }
}