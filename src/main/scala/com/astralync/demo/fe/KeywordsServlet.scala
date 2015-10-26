package com.astralync.demo.fe

import java.io.Serializable
import java.net.InetAddress
import javax.servlet.ServletConfig

import com.astralync.demo.spark.web.HttpUtils
import org.apache.spark.SparkContext
import org.scalatra.scalate.ScalateSupport
import org.slf4j.LoggerFactory

import scala.xml.Node

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * KeywordsServlet
 *
 */
class KeywordsServlet extends KeywordsStack with Serializable with ScalateSupport {

  val logger = LoggerFactory.getLogger(getClass)
  var sc: SparkContext = _
  var cacheEnabled = true
  var nLoops = 1
  val InteractionField = "interaction_created_at"
  val DtNames = s"twitter_user_location twitter_user_lang $InteractionField"
  val fakeArgs = "local[32] /shared/demo/data/dataSmall 3 3 true".split(" ")

  override def init(config: ServletConfig) = {
    super.init(config)
  }

  val headers = List(InteractionField, "interaction_content", "interaction_geo_latitude", "interaction_geo_longitude", "interaction_id", "interaction_author_username", "interaction_link", "klout_score", "interaction_author_link", "interaction_author_name", "interaction_source", "salience_content_sentiment", "datasift_stream_id", "twitter_retweeted_id", "twitter_user_created_at", "twitter_user_description", "twitter_user_followers_count", "twitter_user_geo_enabled", "twitter_user_lang", "twitter_user_location", "twitter_user_time_zone", "twitter_user_statuses_count", "twitter_user_friends_count", "state_province")

  private def displayPage(title: String, content: Seq[Node]) = Template.page(title, content, url(_))

  val RegexUrl = s"http://${InetAddress.getLocalHost.getHostName}:8180/"
  post("/runQuery") {

    try {

      val args = params("cmdline").split("\\s").map(_.trim)
      val JsonPosRegex: String = if (params.contains("posKeywords")) {
        params("posKeywords").split(" ").map(_.trim).filter(_.length > 0).map(k =>
          s""" "$k":"(?i)(.*$k.*)" """)
          .mkString("{", ",", "}")
      } else {
        throw new IllegalArgumentException("Missing the keyword parameter jsonPos")
      }
      println(s"${JsonPosRegex}")
      println(s"${JsonPosRegex.getClass.getSimpleName}")

      val JsonNegRegex: String = if (params.contains("negKeywords")) {
        params("negKeywords").split(" ").map(_.trim).filter(_.length > 0).map(k =>
          s""" "$k":"(?i)(.*$k.*)" """)
          .mkString("{", ",", "}")

      } else {
        throw new IllegalArgumentException("Missing the keyword parameter jsonNeg")
      }
      println(s"${JsonNegRegex}")
      println(s"${JsonNegRegex.getClass.getSimpleName}")

      val headersOrderMap = (0 until headers.length).zip(headers).toMap
      val headersNameMap = headers.zip(0 until headers.length).toMap

      val dataFile = if (args.length >= 2) args(1)
      else throw new IllegalArgumentException("Missing datafile parameter")

      val nparts = if (args.length >= 3) args(2).toInt else 100 // assuming 56 workers - do slightly less than 2xworkers
      val nloops = if (args.length >= 4) args(3).toInt else 3
      val groupByFields = params("grouping").replace(" ", ",")
      val minCount = params("mincount").toInt
      val posRegex = JsonPosRegex
      val negRegex = JsonNegRegex
      val posKeyWords = params("posKeywords").trim.replace(" ",",")
      val negKeyWords = params("negKeywords").trim.replace(" ",",")
      println(s"negKeyWords=[$negKeyWords]")
      var negkeys = negKeyWords
      if (negkeys.trim.length==0) {
        negkeys = "IgnoreMe"
      }
      println(s"negkeys=$negkeys")
      val cmdline = params("cmdline") + Seq("", groupByFields, minCount,
        posKeyWords,params("posKeywordsAndOr"),
        negkeys,params("negKeywordsAndOr")).mkString(" ")
      import collection.mutable
      val rparams = mutable.Map[String, String](params.toSeq: _*)
      rparams.update("cmdline", cmdline)
      rparams.update("jsonNeg", JsonNegRegex)
      rparams.update("jsonPos", JsonPosRegex)
      rparams.update("negKeywords", negkeys)
      val url = RegexUrl
      println(s"Url=$url rparams=${rparams.mkString(",")}")
      val retMapJson = HttpUtils.post(url, Map(rparams.toSeq: _*))
      println(s"retMapJson=$retMapJson")

      val returnMode = "HTML" // rparams("mode")
      if (returnMode != null && !returnMode.trim.isEmpty()) {
        if (returnMode.equalsIgnoreCase("HTML")) {
          displayPage("Keywords Query Results:",
            <pre>{retMapJson}</pre>
              <pre>Route: /query</pre>
          )
        }
        else {
          response.setContentType("application/json")
          response.setContentLength(retMapJson.toString().length())
          response.writer.print(retMapJson.toString())
        }
      }
      else {
        response.setContentType("application/json")
        response.setContentLength(retMapJson.toString().length())
        response.writer.print(retMapJson.toString())
      }
    } catch {
      case e: Exception =>
        System.err.println(s"got exception ${e.getMessage}")
        e.printStackTrace(System.err)
    }

  }

  val title = "Astralync: Twitter Keywords Search"
  get("/") {
    serveStaticResource()
  }

//  get ("/home") {
//    <html>
//      <body>
//        <h1>$
//          {title}
//        </h1>
//        Say
//        <a href="queryForm">Query Form</a>
//        .
//      </body>
//    </html>
//  }

//  get("/home") {
//    get("/query")
//  }

  get("/query") {
    val posKeywords = """wells fargo chase bank money cash"""
    val negKeywords = """dallas arlington"""
    val gval = DtNames
    val sortBy = headers.map(h => s"""<option value="$h">$h</option>""").mkString("\n")
//    println(s"sortBy=$sortBy")
    displayPage(title,
        <table border="0"><tr><td width="60%"><table border="0">
      <form action={url("/runQuery")} method='POST'>
        <tr><td>Included Keywords:<p/>
            <textarea cols="50" rows="3" name="posKeywords">{posKeywords}</textarea></td>
            <td><p/><input type="radio" name="posKeywordsAndOr" value="and">AND</input>
              <p/><input type="radio" name="posKeywordsAndOr" value="or" checked="true">OR</input>
            </td>
        </tr>
         <tr><td>Excluded Keywords:<p/>
         <textarea cols="100" rows="2" name="negKeywords">{negKeywords}</textarea></td>
           <td> <p/> <input type="radio" name="negKeywordsAndOr" value="and" checked="true">AND</input>
              <p/><input type="radio" name="negKeywordsAndOr" value="or">OR</input>
             </td>
          </tr>
          <tr>
            <td colspan="2">Grouping Fields:
              &nbsp; <input type="text" size="80" name="grouping" value={gval.replace(" ", ",")}/>
            </td>
          </tr>
          <tr>
            <td colspan="2">Sort by:
              &nbsp; <select name="sortBy">
                  <option value="twitter_user_location" checked="true">twitter_user_location</option>
                  <option value="interaction_created_at">interaction_created_at</option>
                  <option value="interaction_content">interaction_content</option>
                  <option value="klout_score">klout_score</option>
                  <option value="interaction_author_name">interaction_author_name</option>
                  <option value="interaction_source">interaction_source</option>
                  <option value="twitter_user_created_at">twitter_user_created_at</option>
                  <option value="twitter_user_description">twitter_user_description</option>
                  <option value="twitter_user_followers_count">twitter_user_followers_count</option>
                  <option value="twitter_user_lang">twitter_user_lang</option>
                  <option value="twitter_user_time_zone">twitter_user_time_zone</option>
                  <option value="twitter_user_statuses_count">twitter_user_statuses_count</option>
                  <option value="twitter_user_friends_count">twitter_user_friends_count</option>
                  <option value="state_province">state_province</option>
            </select>
            </td>
          </tr>
          <tr>
            <td colspan="2">MinCount for Groups:
              &nbsp; <input type="text" size="6" name="mincount" value="1"/>
            </td>
          </tr>
          <tr>
            <td colspan="2">Input file:
              <input type="text" size="50" name="inputFile" value="/user/stephen/data5gb"/>
            </td>
            <td width="40%">Save file:
              <input type="text" size="30" name="saveFile" value="results/query"/>
            </td>
            <td width="60%">Export file:
              <input type="text" size="70" name="exportFile" value="src/main/webapp/results/queryResults.csv"/>
              <!-- <input type="text" size="30" name="exportFile" value="src/main/webapp/results/queryResults.csv"/> -->
            </td>
          </tr>
          <tr>
            <td colspan="2">Backend/Spark options:
              <input type="text" size="80" name="cmdline" value="spark://192.168.15.43:7077 hdfs://i386:9000 56 1 true"/>
              <!-- <input type="text" size="80" name="cmdline" value="local[*] /shared/demo/data/data10m 8 1 true"/> -->
            </td>
          </tr>
          <tr>
            <td colspan="2">
              <input type='submit'/>
            </td>
          </tr>
      </form>
         <!-- <tr>
            <td colspan="2">All fields:
              <font size="-1">
                {headers.mkString(", ")}
              </font>
            </td>
          </tr> -->
        </table></td>
          <td width="40%"/></tr></table>
        <pre>Route: /query</pre>
    )
  }

  override def destroy() = {
    if (sc != null) {
      sc.stop()
    }
  }

}

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
                    <a href={url("/query")}>Perform query</a>
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
