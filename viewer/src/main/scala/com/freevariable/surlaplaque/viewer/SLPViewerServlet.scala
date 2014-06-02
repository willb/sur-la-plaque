package com.freevariable.surlaplaque.viewer

import akka.actor.{ActorRef, Actor, ActorSystem}
import akka.util.Timeout

import org.scalatra._
import scalate.ScalateSupport

import scala.concurrent.Await
import scala.concurrent.ExecutionContext

import scala.concurrent.duration._

class SLPViewerServlet(system:ActorSystem, myActor:ActorRef) extends SlpViewerStack with FutureSupport {

  protected implicit def executor: ExecutionContext = system.dispatcher

  import _root_.akka.pattern.ask
  implicit val defaultTimeout = Timeout(10)

  get("/") {
    <html>
      <body>
        <h1>Hello, world!</h1>
        Say <a href="hello-scalate">hello to Scalate</a>.
      </body>
    </html>
  }
  
  get("/cache/:id") {
    val future = myActor ? MAPGET(params("id").toInt)
    Await.result(future, Duration(10000, "millis")).toString
  }
  
}
