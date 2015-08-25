package com.astralync.demo

//import net.liftweb.json.{NoTypeHints, Serialization}
import org.scalatra.scalate.ScalateSupport
import org.slf4j.LoggerFactory

import scala.xml.Node

import java.io.Serializable
import java.util.Date
import javax.servlet._

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

import scala.collection.mutable
import scala.collection.mutable.{Map => MMap}
import scala.util.matching.Regex
import scala.util.parsing.json._

class KeywordsServlet extends KeywordsStack  with Serializable /* with JacksonJsonSupport */ with ScalateSupport {

//  implicit val formats = Serialization.formats(NoTypeHints)

  val logger = LoggerFactory.getLogger(getClass)
  var sc: SparkContext = _
  var rddData: RDD[String] = _
  val CacheEnabled = true

  override def init(config: ServletConfig) = {
    super.init(config)
    // val master = s"spark://${java.net.InetAddress.getLocalHost.getHostName}:7077"
    // var conf = new SparkConf().setMaster("spark://192.168.15.43:7077").setAppName("Connector Stable")
    var conf = new SparkConf().setMaster("local[32]").setAppName("Connector Stable")
    conf.set("spark.driver.memory", "10g")
    sc = new SparkContext(conf)
    println(s"Connecting to master=${conf.get("spark.master")}..")
  }

  private def displayPage(title: String, content: Seq[Node]) = Template.page(title, content, url(_))

  get("/") {
    <html>
      <body>
        <h1>Astralync: Twitter Keywords Search Example</h1>
        Say
        <a href="queryForm">Query Form</a>
        .
      </body>
    </html>
  }


  get("/queryForm") {
    var jsonPos = """
{"Party":"(?i-mx:(\b(party|parties)\b))",
          "New Years Eve":"(?i-mx:(\b(new)\b))",
          "Beer":"(?i-mx:(\b(beer|drunk|drink)\b))",
          "Resolutions":"(?i-mx:(\b(resolution|resolv)\b))"}

                  """.stripMargin

    var jsonNeg = """
{"Party":"(?i-mx:(\b(birthday)\b))",
          "Beer":"(?i-mx:(\b(bud|budweiser)\b))"}
                  """.stripMargin
    displayPage("OpenChai: Argus Query Example",
      <form action={url("/query")} method='POST'>
        JsonPos:
        <textarea cols="100" rows="15" name="jsonPos">
          {jsonPos}
        </textarea>
        <p/>
        JsonNeg:
        <textarea cols="100" rows="15" name="jsonNeg">
          {jsonNeg}
        </textarea>
        <p/>
        Mode:
        <input type="radio" name="mode" label="HTML" value="html"/>
        HTML
        <input type="radio" name="mode" label="JSON" value="json"/>
        JSON
        <p/>
        <input type='submit'/>
      </form>
        <pre>Route: /queryForm</pre>
    )
  }


  get("/demo") {

    contentType = "text/html"

  }


  post("/query") {

    try {
      //val data="hdfs://i386:9000/user/stephen/argus/dataSmall"
      val data = "file:///home/stephen/argus/dataSmall"
      val args: Array[String] = Array("local[32]", /* "spark://192.168.15.43:7077", */ data,
        "2", "1", "false", "/home/stephen/argus/src/posRegex.json", "/home/stephen/argus/src/negRegex.json")
      val DtName = "interaction_created_at"
      // John you need to set this to an input HttpReqParam
      val JsonPosRegex: String = if (params.contains("jsonPos")) {
        params("jsonPos")
      } else {
        """{"Party":"(?i-mx:(\\b(party|parties)\\b))",
              "New Years Eve":"(?i-mx:(\\b(new)\\b))",
              "Beer":"(?i-mx:(\\b(beer|drunk|drink)\\b))",
              "Resolutions":"(?i-mx:(\\b(resolution|resolv)\\b))"}"""
      }
      println(s"${JsonPosRegex}");
      println(s"${JsonPosRegex.getClass.getSimpleName}");

      // John you need to set this to an input HttpReqParam
      val JsonNegRegex: String = if (params.contains("jsonNeg")) {
        params("jsonNeg")
      } else {
        """{"Party":"(?i-mx:(\\b(birthday)\\b))",
          "Beer":"(?i-mx:(\\b(bud|budweiser)\\b))"}
        """
      }
      println(s"${JsonNegRegex}");
      println(s"${JsonNegRegex.getClass.getSimpleName}");

      val headers = List("interaction_created_at", "interaction_content", "interaction_geo_latitude", "interaction_geo_longitude", "interaction_id", "interaction_author_username", "interaction_link", "klout_score", "interaction_author_link", "interaction_author_name", "interaction_source", "salience_content_sentiment", "datasift_stream_id", "twitter_retweeted_id", "twitter_user_created_at", "twitter_user_description", "twitter_user_followers_count", "twitter_user_geo_enabled", "twitter_user_lang", "twitter_user_location", "twitter_user_time_zone", "twitter_user_statuses_count", "twitter_user_friends_count", "state_province")
      val headersOrderMap = (0 until headers.length).zip(headers).toMap
      val headersNameMap = headers.zip(0 until headers.length).toMap

      if (args.length == 0) {
        System.err.println( """Usage: RegexFilters <master> <datadir> <#partitions> <#loops> <cacheEnabled true/false> <groupByFields separated by commas no spaces>
        e.g. RegexFilters spark://192.168.15.43:7077 hdfs://i386:9000/user/stephen/data 56 3 true posregexFile.json negregexfile.json interaction_created_at,state_province""")
        System.exit(1)
      }
      val homeDir = "/home/stephen"
      val dataFile = if (args.length >= 2) args(1) else s"$homeDir/argus/CitiesBaselineCounts2015010520150112.csv"
      val nparts = if (args.length >= 3) args(2).toInt else 100 // assuming 56 workers - do slightly less than 2xworkers
      val nloops = if (args.length >= 4) args(3).toInt else 3
      val cacheEnabled = CacheEnabled // if (args.length >= 5) args(4).toBoolean else false
      // GROUP BY FIELDS: will drive the grouping key's!
      val posRegex = /* if (args.length >= 6) scala.io.Source.fromFile(args(5)).mkString("") else */ JsonPosRegex
      val negRegex = /* if (args.length >= 7) scala.io.Source.fromFile(args(6)).mkString("") else */ JsonNegRegex
      val groupByFields = if (args.length >= 8) args(7).split(",").toList else List()
      // var conf = new SparkConf("/home/stephen/spark-1.4.2/conf/spark-defaults.conf").setAppName("Argus") // .setMaster(master)
      //var conf = new SparkConf(true).setAppName("Argus") // .setMaster(master)
      println(s"Connecting to master=${sc.getConf.get("spark.master")} reading dir=$dataFile using nPartitions=$nparts and caching=$cacheEnabled .. ")
      println(s"PosRegex=$posRegex\nNegRegex=$negRegex")
      if (rddData == null) {
        rddData = sc.textFile(dataFile, nparts)
      }
      if (cacheEnabled) {
        rddData.cache()
      }

      val durations = mutable.ArrayBuffer[Double]()
      var countedRdd: Map[String, Long] = null
      for (nloop <- (0 until nloops)) {
        val d = new Date
        println(s"** Loop #${nloop + 1} starting at $d **")
        val resultMap = MMap[String, Any]()
        val jsonPos = JSON.parseFull(posRegex)
        val posRegexMap = jsonPos.map { m =>
          val p = m.asInstanceOf[Map[String, Any]]
          p.map { case (k, v) =>
            (k, v.asInstanceOf[String].r)
          }
        }
        val jsonNeg = JSON.parseFull(negRegex)
        val negRegexMap = jsonNeg.map { m =>
          m.asInstanceOf[Map[String, Any]].map { case (k, v) =>
            (k, v.asInstanceOf[String].r)
          }
        }

        val bc = sc.broadcast((posRegexMap, negRegexMap, headers, groupByFields))
        val filtersRdd = rddData.mapPartitionsWithIndex({ case (rx, iter) =>
          val (locPosMap, locNegMap, locHeaders, locGroupingFields) = bc.value
          def parseLine(iline: String, headersList: List[String]):
          Option[Map[String, String]] = {
            val line = iline.trim
            def formatDate(dt: String) = {
              s"${dt.slice(0, 4)}-${dt.slice(8, 10)}-${dt.slice(5, 7)}"
            }
            if (line.length == 0 || line.count(c => c == ',') != locHeaders.size) {
              None
            } else {
              val lmap = locHeaders.zip(line.split(",").map(_.replace("\"", ""))).toMap
              val nmap = lmap.updated(DtName, formatDate(lmap(DtName)))
              Some(nmap)
            }
          }
          def genKey(k: String, groupingFields: List[String], lmap: Map[String, String]) = {
            if (groupingFields.length == 0) {
              k
            } else {
              val groupKey = groupingFields.map(f => lmap(f)).mkString("-")
              s"$k->$groupKey"
            }
          }

          var nlines = 0
          iter.map { line =>
            nlines += 1
            val posFiltered = for ((k, regex) <- locPosMap.get) yield {
              val rout = regex.findFirstIn(line) match {
                case Some(l) =>
                  val allGood = if (locNegMap.isEmpty) {
                    true
                  } else {
                    locNegMap.get.foldLeft(true) { case (stillgood, (k, negRegex)) =>
                      val rout = negRegex.findFirstIn(l) match {
                        case Some(nl) => false
                        case _ => stillgood
                      }
                      rout
                    }
                  }
                  if (allGood) {
                    val lmap = parseLine(line, locHeaders)
                    if (lmap.isDefined) {
                      val oval = genKey(k, locGroupingFields, lmap.get)
                      println(s"accepting line $line")
                      Some((oval, 1))
                    } else {
                      None
                    }
                  } else {
                    None
                  }
                case _ => None
              }
              rout
            }
            println(s"For partition=$rx  number of lines processed=$nlines")
            posFiltered.toList.flatten
          }
        }, true)
        val outRdd = filtersRdd.filter(l => l.length > 0 && !l.isEmpty)
        //println(outRdd.take(20).mkString(","))
        //outRdd.take(20).foreach{ l => println(s"len is ${l.length}")}
        val lrdd = outRdd.flatMap { x => x }.countByKey()
        countedRdd = Map(lrdd.toList: _*)
        val duration = ((new Date().getTime - d.getTime) / 100).toInt / 10.0
        durations += duration
        println(s"** RESULT for loop ${nloop + 1}:  ${countedRdd.mkString(",")} duration=$duration secs *** ")
      }
      println(s"** Test Completed. Loop durations=${durations.mkString(",")} ** ")
      if (!cacheEnabled) {
        rddData = null
      }
      var retMapJson = new JSONObject(Map[String, Any](countedRdd.toSeq: _*))
      val returnMode = params("mode")
      if (returnMode != null && !returnMode.trim.isEmpty()) {
        if (returnMode.equalsIgnoreCase("HTML")) {
          displayPage("Argus Query Results:",
            // <p>Query: {jsonObject.get.toString}</p>
            <p>Return:
              {retMapJson}
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
        <link href="/assets/css/scalatra.css" rel="stylesheet"/>

        {head}
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
