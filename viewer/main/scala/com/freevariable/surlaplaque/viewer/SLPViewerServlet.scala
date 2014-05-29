package com.freevariable.surlaplaque.viewer

import org.scalatra._
import scalate.ScalateSupport

class SLPViewerServlet extends SlpViewerStack {

  get("/") {
    <html>
      <body>
        <h1>Hello, world!</h1>
        Say <a href="hello-scalate">hello to Scalate</a>.
      </body>
    </html>
  }
  
}
