package com.astralync.demo.fe

import java.io.Serializable
import java.net.{URLEncoder, InetAddress}
import javax.servlet._

import com.astralync.demo.spark.web.HttpUtils
import org.apache.spark.SparkContext
import org.scalatra.scalate.ScalateSupport
import org.slf4j.LoggerFactory

import scala.collection.mutable.{Map => MMap}
import scala.util.matching.Regex
import scala.xml.Node



object Matcher {
  def matchRegex(line: String, regex: Regex): Boolean = {
    regex findFirstIn (line) match {
      case Some(l) => return true
      case None => return false
    }
  }
}

object Template {

  def page(title: String, content: Seq[Node], url: String => String = identity _, head: Seq[Node] = Nil, scripts: Seq[String] = Seq.empty, defaultScripts: Seq[String] = Seq("js/jquery.min.js", "js/bootstrap.min.js")) = {
    <html lang="en">
      <head>
        <title>
          {title}
        </title>
        <meta charset="utf-8"/>
        <meta name="viewport" content="width=device-width, initial-scale=1.0"/>
        <meta name="description" content=" "/>
        <meta name="author" content=" "/>

        <!-- Styles -->
        <link href="css/normalize.css" rel="stylesheet"/>
      </head>

      <body>
        <div class="navbar navbar-inverse navbar-fixed-top">
          <div class="navbar-inner">
            <div class="container">
              <a class="btn btn-navbar" data-toggle="collapse" data-target=".nav-collapse">
                <span class="icon-bar"></span>
                <span class="icon-bar"></span>
                <span class="icon-bar"></span>
              </a>
              <a class="brand" href="/">Argus Test</a>
              <div class="nav-collapse collapse">

              </div> <!--/.nav-collapse -->
            </div>
          </div>
        </div>

        <div class="container">
          <div class="content">
            <div class="page-header">
              <h1>
                {title}
              </h1>
            </div>
            <div class="row">
              <div class="span3">
                <ul class="nav nav-list">
                  <li>
                    <a href={url("/queryForm")}>Perform query</a>
                  </li>
                </ul>
              </div>
              <div class="span9">
                {content}
              </div>
              <hr/>
            </div>
          </div> <!-- /content -->
        </div> <!-- /container -->
        <footer class="vcard" role="contentinfo">

        </footer>{(defaultScripts ++ scripts) map { pth =>
        <script type="text/javascript" src={pth}></script>
      }}

      </body>

    </html>
  }
}
