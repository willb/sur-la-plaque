package com.freevariable.surlaplaque.viewer

import akka.actor.{ActorRef, Actor, ActorSystem}
import akka.util.Timeout

import org.scalatra._
import scalate.ScalateSupport

import scala.concurrent.Await
import scala.concurrent.ExecutionContext

import scala.concurrent.duration._

class SLPViewerServlet(system:ActorSystem, cache:ActorRef) extends SlpViewerStack with FutureSupport {

  protected implicit def executor: ExecutionContext = system.dispatcher

  import _root_.akka.pattern.ask
  implicit val defaultTimeout = Timeout(10)

  get("/") {
    <html>
    </html>
  }
  
  get("/cache/:id") {
    val key = params("id").toString
    val future = cache ? GetCommand(key)
    val result = Await.result(future, Duration(100, "millis"))
    result match {
      case GenericDocument(doc) => {
        contentType = "application/json"
        Ok(doc)
      }
      case MissingDocument => NotFound(s"couldn't find $key")
    }
  }
  
  get("/view-map/:id") {
    val key = params("id").toString
    val future = cache ? GetCommand(key)
    val result = Await.result(future, Duration(100, "millis"))
    result match {
      case GenericDocument(doc) => {
        contentType = "text/html"
        jade("map", "key" -> key)
      }
      case MissingDocument => NotFound(s"couldn't find $key")
    }
  }
  
  put("/cache/:id") {
    val key = params("id").toString
    val value = request.body
    cache ! PutCommand(params("id").toString, GenericDocument(request.body))
  }
}
