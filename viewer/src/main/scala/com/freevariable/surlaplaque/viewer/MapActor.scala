package com.freevariable.surlaplaque.viewer

import akka.actor.{ActorRef, Actor, ActorSystem}

case object EntryNotFound

sealed abstract class MapCommand
case class MAPGET(val k: Int) extends MapCommand
case class MAPPUT(val k: Int, val v: Any) extends MapCommand

class MapActor extends Actor {
  val PUT = Symbol("put")
  val GET = Symbol("get")
  
  private var cache = Map[Int, Any](1 -> "foo")
  
  def receive = {
    case MAPPUT(k:Int, v:Any) => cache = cache + Pair(k, v)
    case MAPGET(k:Int) => sender ! cache.getOrElse(k, EntryNotFound).toString
    case x => sender ! s"$x IS NOT RECOGNIZED BUT THE DUDE ABIDES"
  }
}