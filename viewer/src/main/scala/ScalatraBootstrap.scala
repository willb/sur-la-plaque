import akka.actor.{ActorSystem, Props}

import com.freevariable.surlaplaque.viewer._
import org.scalatra._
import javax.servlet.ServletContext

class ScalatraBootstrap extends LifeCycle {
  // Get a handle to an ActorSystem and a reference to one of your actors
  val system = ActorSystem()
  val documentCache = system.actorOf(Props[DocumentCache])
  
  override def init(context: ServletContext) {
    context.mount(new SLPViewerServlet(system, documentCache), "/*")
  }
}
