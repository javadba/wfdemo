package com.astralync.demo

//import net.liftweb.json.{NoTypeHints, Serialization}

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

class KeywordsServlet extends KeywordsStack with Serializable with ScalateSupport {

  val logger = LoggerFactory.getLogger(getClass)
  var sc: SparkContext = _
  var cacheEnabled = true
  var nLoops = 1
  var groupByFields = List[String]()
  val InteractionField = "interaction_created_at"
  val DtNames = s"$InteractionField twitter_user_lang"
//  val DtNames = s"$InteractionField state_province"
  //  val fakeArgs = "spark://192.168.15.43:7077 hdfs://i386:9000/user/stephen/data 56 3 true".split(" ")
  val fakeArgs = "local[32] /shared/demo/data/dataSmall 3 3 true".split(" ")

  override def init(config: ServletConfig) = {
    super.init(config)
  }

  val headers = List(InteractionField, "interaction_content", "interaction_geo_latitude", "interaction_geo_longitude", "interaction_id", "interaction_author_username", "interaction_link", "klout_score", "interaction_author_link", "interaction_author_name", "interaction_source", "salience_content_sentiment", "datasift_stream_id", "twitter_retweeted_id", "twitter_user_created_at", "twitter_user_description", "twitter_user_followers_count", "twitter_user_geo_enabled", "twitter_user_lang", "twitter_user_location", "twitter_user_time_zone",  "twitter_user_statuses_count", "twitter_user_friends_count",  "state_province")

  private def displayPage(title: String, content: Seq[Node]) = Template.page(title, content, url(_))

  val title = "Astralync: Twitter Keywords Search"
  get("/") {
    <html>
      <body>
        <h1>$
          {title}
        </h1>
        Say
        <a href="queryForm">Query Form</a>
        .
      </body>
    </html>
  }


  get("/queryForm") {
    val posKeywords = """iphone twitter love boyz"""

    val negKeywords = """don't hate parkside"""
    val gval = DtNames
    displayPage(title,
      <form action={url("/query")} method='POST'>
        <table>
        <tr><td>Included Keywords:</td><td>Excluded Keywords:</td></tr>
        <tr><td> <textarea cols="50" rows="3" name="jsonPos">
          {posKeywords}
        </textarea></td>
         <td> <textarea cols="100" rows="2" name="jsonNeg"> {negKeywords} </textarea></td>
          </tr>
          </table>
        <p/>
        Mode:
        <input type="radio" name="mode" label="HTML" value="html"/>
        HTML
        <input type="radio" name="mode" label="JSON" value="json" checked="true"/>
        JSON
        <p/>
        Grouping Fields:
        <input type="text" size="80" name="grouping" value={gval}/>
        <p/>
        <p>All fields:
          <font size="-1">
            {headers.mkString(" ,")}
          </font>
        </p>
        <p/>
        MinCount for Groups:&nbsp;
          <input type="text" size="6" name="mincount" value="20"/>
        <p/>
        Backend/Spark options:
        <input type="text" size="80" name="cmdline" value="local[*] /shared/demo/data/data500m 4 1 true"/>
        <!--        Backend/Spark options: <input type="text" name="cmdline">spark://192.168.15.43:7077 hdfs://i386:9000/user/stephen/data 56 3 true</input> -->
        <p/>
        <input type='submit'/>
      </form>
        <pre>Route: /queryForm</pre>
    )
  }


  get("/demo") {

    contentType = "text/html"

  }


  val RegexUrl=s"http://${InetAddress.getLocalHost.getHostName}:8180/wfdemo"
  post("/query") {

    try {

      val args = params("cmdline").split("\\s").map(_.trim)
      //val data="hdfs://i386:9000/user/stephen/argus/dataSmall"
//      val data = "file:///shared/demo/data/dataSmall"
//      val args: Array[String] = Array("local[32]", /* "spark://192.168.15.43:7077", */ data,
//        "2", "1", "false", "/home/stephen/argus/src/posRegex.json", "/home/stephen/argus/src/negRegex.json")
      // John you need to set this to an input HttpReqParam
      val JsonPosRegex: String = if (params.contains("jsonPos")) {
        params("jsonPos").split(" ").map(_.trim).filter(_.length > 0).map(k =>
          s""" "$k":"(?i)(.*$k.*)" """)
          .mkString("{", ",", "}")
      } else {
        throw new IllegalArgumentException("Missing the keyword parameter jsonPos")
      }
      println(s"${JsonPosRegex}");
      println(s"${JsonPosRegex.getClass.getSimpleName}");

      // John you need to set this to an input HttpReqParam
      val JsonNegRegex: String = if (params.contains("jsonNeg")) {
        params("jsonNeg").split(" ").map(_.trim).filter(_.length > 0).map(k =>
          s""" "$k":"(?i)(.*$k.*)" """)
          .mkString("{", ",", "}")

      } else {
        throw new IllegalArgumentException("Missing the keyword parameter jsonNeg")
      }
      println(s"${JsonNegRegex}");
      println(s"${JsonNegRegex.getClass.getSimpleName}");

      val headersOrderMap = (0 until headers.length).zip(headers).toMap
      val headersNameMap = headers.zip(0 until headers.length).toMap

      if (args.length == 0) {
        System.err.println( """Usage: RegexFilters <master> <datadir> <#partitions> <#loops> <cacheEnabled true/false> <groupByFields separated by commas no spaces>
        e.g. RegexFilters spark://192.168.15.43:7077 hdfs://i386:9000/user/stephen/argus/data500mb 56 3 true posregexFile.json negregexfile.json interaction_created_at,state_province""")
        System.exit(0)
      }
      val homeDir = "/home/stephen"
      val dataFile = if (args.length >= 2) args(1)
          else throw new IllegalArgumentException("Missing datafile parameter")

      val nparts = if (args.length >= 3) args(2).toInt else 100 // assuming 56 workers - do slightly less than 2xworkers
      val nloops = if (args.length >= 4) args(3).toInt else 3
      // GROUP BY FIELDS: will drive the grouping key's!
      val posRegex = /* if (args.length >= 6) scala.io.Source.fromFile(args(5)).mkString("") else */ JsonPosRegex
      val negRegex = /* if (args.length >= 7) scala.io.Source.fromFile(args(6)).mkString("") else */ JsonNegRegex
      val groupByFields = params("grouping").split("\\s").toSeq
      val minCount = params("mincount").toInt
      // var conf = new SparkConf("/home/stephen/spark-1.4.2/conf/spark-defaults.conf").setAppName("Argus") // .setMaster(master)
      //var conf = new SparkConf(true).setAppName("Argus") // .setMaster(master)

      val cmdline = URLEncoder.encode(params("cmdline") + Seq("",JsonPosRegex, JsonNegRegex, groupByFields, minCount).mkString(" "))
      import collection.mutable
      val rparams = mutable.Map[String,String](params.toSeq:_*)
      rparams.update("cmdline", cmdline)
//      val eparams = params.mapValues( pv => URLEncoder.encode(pv))
//      val url = s"$RegexUrl?cmdline=$cmdline&mincount=$minCount&groupByFields=${eparams("grouping")}"
      val url = RegexUrl
      println(s"Url=$url")
      val retMapJson = HttpUtils.post(url, Map(rparams.toSeq:_*))
      val returnMode = rparams("mode")
      if (returnMode != null && !returnMode.trim.isEmpty()) {
        if (returnMode.equalsIgnoreCase("HTML")) {
          displayPage("Keywords Query Results:",
            // <p>Query: {jsonObject.get.toString}</p>
            <p>Return:
              {retMapJson.toList}
            </p>
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
        System.err.println("got exception")
        e.printStackTrace
    }

  }

  override def destroy() = {
    if (sc != null) {
      sc.stop()
    }
  }

}

object Matcher {
  def matchRegex(line: String, regex: Regex): Boolean = {
    regex findFirstIn (line) match {
      case Some(l) => return true
      case None => return false
    }
  }
}

object Template {

  def page(title: String, content: Seq[Node], url: String => String = identity _, head: Seq[Node] = Nil, scripts: Seq[String] = Seq.empty, defaultScripts: Seq[String] = Seq("/assets/js/jquery.min.js", "/assets/js/bootstrap.min.js")) = {
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
        <link href="/assets/css/bootstrap.css" rel="stylesheet"/>
        <link href="/assets/css/bootstrap-responsive.css" rel="stylesheet"/>
        <link href="/assets/css/syntax.css" rel="stylesheet"/>
        <link href="/assets/css/scalatra.css" rel="stylesheet"/>{head}
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
