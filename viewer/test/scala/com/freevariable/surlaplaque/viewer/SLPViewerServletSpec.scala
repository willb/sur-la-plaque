package com.freevariable.surlaplaque.viewer

import org.scalatra.test.specs2._

// For more on Specs2, see http://etorreborre.github.com/specs2/guide/org.specs2.guide.QuickStart.html
class SLPViewerServletSpec extends ScalatraSpec { def is =
  "GET / on SLPViewerServlet"                     ^
    "should return status 200"                  ! root200^
                                                end

  addServlet(classOf[SLPViewerServlet], "/*")

  def root200 = get("/") {
    status must_== 200
  }
}
