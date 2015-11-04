package com.astralync.demo.spark

import java.net.{URLEncoder, InetAddress}
import java.text.SimpleDateFormat
import java.util.Date

import com.astralync.demo.TwitterLineParser
import com.astralync.demo.spark.web.DemoHttpServer
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable
import scala.collection.mutable.{Map => MMap}
import scala.util.parsing.json._


/**
 * RegexFilters
 *
 */
object RegexFilters {

  val DefaultPort = 8180
  val WebPort = 9090
  val DefaultCtx = "/"
  var rddData: RDD[String] = _
  var cacheEnabled = true
  var nLoops = 1
  var groupByFields = List[String]()
  val InteractionField = "interaction_created_at"
  val DtNames = s"$InteractionField twitter_user_lang"
  val SearchTerms="search_terms"
  val MaxLines = 200

  def main(args: Array[String]) = {
    startWebServer
    Thread.currentThread.join
  }

  private var webServer: DemoHttpServer = _

  def startWebServer() = {
    webServer = new DemoHttpServer().run(Map("port" -> DefaultPort, "ctx" -> DefaultCtx))
  }

  var sc: SparkContext = _

  def deleteFile(furl: String) = {
    val hadoopConf = new org.apache.hadoop.conf.Configuration()

    val hdfs = org.apache.hadoop.fs.FileSystem.get(new java.net.URI(furl), hadoopConf)

    try {
      hdfs.delete(new org.apache.hadoop.fs.Path(furl), true)
    } catch {
      case e: Exception => { e.printStackTrace }
    }

  }
  def getSc(master: String) = {
    if (sc == null) {
      var conf = new SparkConf().setAppName("Simple Application").setMaster(master)
      println(s"Connecting to master=${conf.get("spark.master")}")
      sc = new SparkContext(conf)
    }
    sc
  }

  def submit(params: Map[String,String]) = {
    try {
      println(s"Submit entered with ${params.toSeq.mkString(",")}")
      val DtName = "interaction_created_at"

      if (params.isEmpty) {
        System.err.println( """Usage: RegexFilters <master> <datadir> <#partitions> <#loops> <cacheEnabled true/false> <groupByFields separated by commas no spaces>
        e.g. RegexFilters spark://192.168.15.43:7077 hdfs://i386:9000/user/stephen/data 56 3 true posregexFile.json negregexfile.json interaction_created_at,state_province""")
        System.exit(1)
      }
      var k = 0
      val query= params("query")
      val master  = params("sparkMaster")
      val namenode = params("nameNode")

      val nparts  = params("nparts").toInt
      val nloops  = params("nloops").toInt
      val cacheEnabled = params("cacheEnabled").equalsIgnoreCase("on")
      val groupByFields = params("grouping").split(",").toList
      val minCount = params("minCount").toInt
      val posKeywords = params("posKeywords").split(" ").toList
      val posAndOr = params("posKeywordsAndOr")
      val negKeywords = params("negKeywords").split(" ").toList
      val negAndOr = params("negKeywordsAndOr")
      val sortBy = params("sortBy")
      val df = new SimpleDateFormat("MMdd-HHmmss")
      val datef = df.format(new java.util.Date())
      val inputFile = s"""$namenode/${params("inputFile")}"""
      var saveFileBare = params("saveFile")
      var saveFile = s"$namenode/$saveFileBare"
      if (saveFile == inputFile) {
        saveFile = saveFile + datef
      }
      val exportFile = params("exportFile")
      val urlNoRoute =  s"http://${InetAddress.getLocalHost.getHostName}:$WebPort"
      val url =  s"$urlNoRoute/runQuery"
      println(s"Reading dir=$inputFile using nPartitions=$nparts and caching=$cacheEnabled .. ")
      sc = getSc(master)
      deleteFile(saveFile)
      var rddData = sc.textFile(inputFile, nparts)
      if (cacheEnabled) {
        rddData.cache()
      }

      val queryParams = query.split("&")
      val searchTerms :Option[String] = if (params.contains(SearchTerms)) {
        Some(params(SearchTerms))
      } else {
        None
      }
      val doDrillDown = !searchTerms.isEmpty
      case class LoopOutput(nrecs: Int, saveDuration: Double, duration: Double)
      val loopOut = mutable.ArrayBuffer[LoopOutput]()
      var countedRdd: Map[String, Long] = null
      var filteredRdd: RDD[String] = null
      for (nloop <- (0 until nloops)) {
        val saved = new Date
        println(s"** Loop #${nloop + 1} starting at $saved **")
        val resultMap = MMap[String, Any]()

        val bc = sc.broadcast(searchTerms, posKeywords, posAndOr, negKeywords, negAndOr, TwitterLineParser.headers, groupByFields, DtNames,
          InteractionField, minCount)
         val accum = sc.accumulator(0)
        val saveFiles = !saveFile.isEmpty || !exportFile.isEmpty
        filteredRdd = if (saveFiles) {
          val savedRdd = rddData.mapPartitionsWithIndex({ case (rx, iter) =>
            val (locSearchTerms, locPosKeywords, locPosAndOr, locNegKeywords, locNegAndOr, locHeaders, locGroupingFields, locDtNames,
            locInteractionField, locMinCount) = bc.value
            val doDrillDown = !locSearchTerms.isEmpty

            assert(locPosKeywords.nonEmpty, "Must supply some positive keywords")
            var nlines = 0

            val (posAndOr, lowerPosKeywords,lowerNegKeywords) = if (doDrillDown) {

              ("AND", locSearchTerms.get.split(" ").toList, locNegKeywords.map(_.toLowerCase))
            } else {
              (locPosAndOr, locPosKeywords.map(_.toLowerCase), List[String]())
            }
            val iterout = iter.map { line =>
              nlines += 1
              //            println(line)
              val outKeys = if (posAndOr.equalsIgnoreCase("OR")) {
                lowerPosKeywords.filter(line.contains(_))
              } else {
                val allFound = lowerPosKeywords.foldLeft(true) { case (b, k) => b && line.contains(k) }
                if (allFound) {
                  List(lowerPosKeywords.mkString(":"))
                } else {
                  List[String]()
                }
              }
              val allGood = outKeys.nonEmpty && (lowerNegKeywords.isEmpty || {
                if (locNegAndOr.equalsIgnoreCase("OR")) {
                  lowerNegKeywords.foldLeft(true) { case (b, k) => b && !line.contains(k) }
                } else {
                  lowerNegKeywords.foldLeft(false) { case (b, k) => b || !line.contains(k) }
                }
              })
              if (nlines % 1000 == 1) {
                println(s"For partition=$rx  number of lines processed=$nlines")
              }
              if (!iter.hasNext) {
                accum += nlines
              }
              if (allGood) {
                line
              } else {
                ""
              }
            }
            iterout
          }, true).filter(_.trim.length > 0)
          if (!saveFile.isEmpty) {

            savedRdd.saveAsTextFile(saveFile)
            println(s"Saved ${accum.value} lines to $saveFile")
          }
          if (!exportFile.isEmpty) {
            val results = savedRdd.collect.mkString("\n")
            tools.nsc.io.File(exportFile).writeAll(results)
            println(s"Saved ${accum.value} lines to $exportFile")
          }
          savedRdd
        } else {
          null.asInstanceOf[RDD[String]]
        }
        val saveDuration = ((new Date().getTime - saved.getTime) / 100).toInt / 10.0
        val groupingStart = new Date

        var groupingRdd : RDD[(String,Int)] = null
        if (!doDrillDown) {
          val groupingInput = if (filteredRdd!=null) filteredRdd else rddData

          groupingRdd = groupingInput.mapPartitionsWithIndex({ case (rx, iter) =>
            val (locSearchTerms, locPosKeywords, locPosAndOr, locNegKeywords, locNegAndOr, locHeaders, locGroupingFields, locDtNames,
            locInteractionField, locMinCount) = bc.value
            def genKey(k: String, groupingFields: Seq[String], lmap: Map[String, String]) = {
              if (groupingFields.isEmpty) {
                k
              } else {
                val sortedFields = if (!groupingFields.contains(sortBy)) {
                  System.err.println(s"Must select sortBy field contained in Grouping fields: sortBy=$sortBy")
                  groupingFields
                } else {
                  List(sortBy) ++ groupingFields diff List(sortBy)
                }

                val groupKey = sortedFields.map { f =>
                  if (!lmap.contains(f)) {
                    System.err.println(s"key $f not found in lmap=${lmap.mkString(",")}")
                    k
                  } else {
                    lmap(f)
                  }
                }
                (k +: groupKey).mkString(" ")
              }
            }

            val (lowerPosKeywords,lowerNegKeywords) = if (doDrillDown) {
              (searchTerms.get.split(",").toList, locNegKeywords.map(_.toLowerCase))
            } else {
              (locPosKeywords.map(_.toLowerCase), List[String]())
            }
            val iterout = iter.map { line =>
              val outKeys = if (posAndOr.equalsIgnoreCase("OR")) {
                lowerPosKeywords.filter(line.contains(_))
              } else {
                val allFound = lowerPosKeywords.foldLeft(true) { case (b, k) => b && line.contains(k) }
                if (allFound) {
                  List(lowerPosKeywords.mkString(":"))
                } else {
                  List[String]()
                }
              }
                val lmap = TwitterLineParser.parse(line)
                if (lmap.isDefined) {
                  val o = outKeys.map { k =>
                    val oval = genKey(k, locGroupingFields, lmap.get)
                    //                      println(s"accepting line $line")
                    (oval, 1)
                  }
                  o
                } else {
                  List[(String, Int)]()
                }
            }
            iterout
          }, true).flatMap(identity)
        }
        val outRdd = if (!doDrillDown) {
          groupingRdd
        } else {
          filteredRdd
        }
        if (!doDrillDown) {
          val lrdd = groupingRdd.countByKey()
          val filtered = lrdd.filter { case (k, v) => v >= minCount }
          countedRdd = Map(filtered.toList: _*)
        } else {
          val filteredCount = filteredRdd.count
          countedRdd = searchTerms.zip(List.fill(searchTerms.size)(filteredCount)).toMap
        }
        val totalLines = accum.value
        val duration = math.max(((new Date().getTime - groupingStart.getTime) / 1000), 0.001)
        loopOut += LoopOutput(totalLines, saveDuration, duration)
        println(s"** RESULT for loop ${nloop + 1}: #recs=$totalLines ${countedRdd.mkString(",")} duration=$duration secs *** ")
        if (!cacheEnabled) {
          rddData = null
        }
      }
      println(s"** Search Completed. Search durations=${loopOut.map(_.toString).mkString(",")} ** ")
      val cseq = countedRdd.toSeq
      val combo = /* dseq ++ */ cseq
      val outcombo = combo.sortWith { case ((astr, acnt), (bstr, bcnt)) =>
        if (acnt != bcnt) {
          acnt - bcnt >= 0
        } else {
          bstr.substring(bstr.indexOf(" ") + 1).compareTo(astr.substring(astr.indexOf(" ") + 1)) >= 0
        }
      }
      @inline def makeUrl(str: String) = {
        s"""$url?inputFile=${URLEncoder.encode(saveFileBare)}&$SearchTerms=${URLEncoder.encode(str).replace(" ",",")}&$query"""
      }
      val useUrl = true
      val filteredLines = filteredRdd.take(MaxLines).mkString("\n")
      val formatted = (if (useUrl) {
        if (!doDrillDown) {
          outcombo.map { case (str, cnt) =>
            s"""<tr><td>$cnt</td><td><a href="${makeUrl(str)}">$str</a></td></tr>"""
          }
            .mkString( """<table border="0">""", "\n", """</table>""")
        } else {
          outcombo.map { case (str, cnt) =>
            s"""<tr><td><a href="${makeUrl(str)}">$str</a></td></tr>"""
          }
            .mkString( """<table border="0">""", "\n", """</table>""")
        }
      } else {
        outcombo.map { case (str, cnt) => s"($cnt) $str" }.mkString("\n")
      }) + s"\n<pre>${filteredLines}</pre>"

      val outFileLink = s"$urlNoRoute/results/${exportFile.substring(exportFile.lastIndexOf("/")+1)}"
      val withDuration = s"""<p>Search and Save time: <font color="RED">${loopOut.map(_.saveDuration).mkString(" ")}</font> seconds</p>\n\n<p>Aggregation time: <font color="RED">${loopOut.map(_.duration).mkString("\n")}</font> seconds</p>\n\n<p>Searched records: <font color="RED">${loopOut.map(_.nrecs).sum}</font></p>\n\n<p><a href="$outFileLink">Search Results CSV File</a></p>\n<p>Search Results Saved on Cluster: <input type="text" size="80" disabled="disabled" value="hdfs dfs -text $saveFile/*"/></p><p>*** Results ***</p>${formatted}"""
      withDuration
    } catch {
      case e: Exception => println(s"Caught ${e.getMessage}"); e.printStackTrace; e.getMessage
    }
  }

}
