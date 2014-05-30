package com.freevariable.surlaplaque.viewer

import akka.actor.{ActorRef, Actor, ActorSystem}

case object EntryNotFound

class MapActor extends Actor {
  val PUT = Symbol("put")
  val GET = Symbol("get")
  
  private var cache = Map[Int, Any](1 -> "foo")
  
  def receive = {
    case (PUT, k:Int, v:Any) => cache = cache + Pair(k, v)
    case (GET, k:Int) => sender ! cache.getOrElse(k, EntryNotFound).toString
  }
}