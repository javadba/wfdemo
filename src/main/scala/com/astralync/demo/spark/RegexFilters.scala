package com.astralync.demo.spark

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
  val DefaultCtx = "/"
  var rddData: RDD[String] = _
  var cacheEnabled = true
  var nLoops = 1
  var groupByFields = List[String]()
  val InteractionField = "interaction_created_at"
  val DtNames = s"$InteractionField twitter_user_lang"

  def main(args: Array[String]) = {
    startWebServer
    Thread.currentThread.join
  }

  private var webServer: DemoHttpServer = _

  def startWebServer() = {
    webServer = new DemoHttpServer().run(Map("port" -> DefaultPort, "ctx" -> DefaultCtx))
  }

  var sc: SparkContext = _

  def getSc(master: String) = {
    if (sc == null) {
      var conf = new SparkConf().setAppName("Simple Application").setMaster(master)
      println(s"Connecting to master=${conf.get("spark.master")}")
      sc = new SparkContext(conf)
    }
    sc
  }

  def submit(args: Array[String]) = {
    try {
      println(s"Submit entered with ${args.mkString("~")}")
      val DtName = "interaction_created_at"

      if (args.length == 0) {
        System.err.println( """Usage: RegexFilters <master> <datadir> <#partitions> <#loops> <cacheEnabled true/false> <groupByFields separated by commas no spaces>
        e.g. RegexFilters spark://192.168.15.43:7077 hdfs://i386:9000/user/stephen/data 56 3 true posregexFile.json negregexfile.json interaction_created_at,state_province""")
        System.exit(1)
      }
      var k = 0
      val homeDir = "/home/stephen"
      val master  = args(k); k+=1
      val dataFile  = args(k); k+=1
      val nparts  = args(k).toInt; k+=1
      val nloops  = args(k).toInt; k+=1
      val cacheEnabled = args(k).toBoolean; k+=1
      val groupByFields = args(k).split(",").toList; k+=1
      val minCount = args(k).toInt ; k+=1
      val posKeywords = args(k).split(",").toList; k+=1
      val posAndOr = args(k); k+=1
      val negKeywords = args(k).split(",").toList; k+=1
      val negAndOr = args(k); k+=1
      val sortBy = args(k); k+=1
      val df = new SimpleDateFormat("MMdd-hhmmss")
      val datef = df.format(new java.util.Date())
      val saveFile = s"${args(k)}.$datef"; k+=1
      val exportFile = args(k).replace(".csv",s".$datef.csv"); k+=1
      val url =  args(k); k+=1
      val searchTerms =  args(k); k+=1
      println(s"Reading dir=$dataFile using nPartitions=$nparts and caching=$cacheEnabled .. ")
      sc = getSc(master)
      var rddData = sc.textFile(dataFile, nparts)
      if (cacheEnabled) {
        rddData.cache()
      }

      case class LoopOutput(nrecs: Int, duration: Double)
      val loopOut = mutable.ArrayBuffer[LoopOutput]()
      var countedRdd: Map[String, Long] = null
      for (nloop <- (0 until nloops)) {
        val d = new Date
        println(s"** Loop #${nloop + 1} starting at $d **")
        val resultMap = MMap[String, Any]()

        val bc = sc.broadcast(posKeywords, posAndOr, negKeywords, negAndOr, TwitterLineParser.headers, groupByFields, DtNames,
          InteractionField, minCount)
         val accum = sc.accumulator(0)
        val filteredRdd = rddData.mapPartitionsWithIndex({ case (rx, iter) =>
          val (locPosKeywords, locPosAndOr, locNegKeywords, locNegAndOr, locHeaders, locGroupingFields, locDtNames,
          locInteractionField, locMinCount) = bc.value

          assert(locPosKeywords.nonEmpty, "Must supply some positive keywords")
          var nlines = 0
          val lowerPosKeywords = locPosKeywords.map(_.toLowerCase)
          val lowerNegKeywords = locNegKeywords.map(_.toLowerCase)
          val iterout = iter.map { line =>
            nlines += 1
            //            println(line)
            val outKeys = if (locPosAndOr.equalsIgnoreCase("OR")) {
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
          filteredRdd.saveAsTextFile(saveFile)
          println(s"Saved ${accum.value} lines to $saveFile")
        }
        if (!exportFile.isEmpty) {
          val results = filteredRdd.collect.mkString("\n")
          tools.nsc.io.File(exportFile).writeAll(results)
          println(s"Saved ${accum.value} lines to $exportFile")
        }

        val groupingRdd = filteredRdd.mapPartitionsWithIndex({ case (rx, iter) =>
          val (locPosKeywords, locPosAndOr, locNegKeywords, locNegAndOr, locHeaders, locGroupingFields, locDtNames,
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

          val lowerPosKeywords = locPosKeywords.map(_.toLowerCase)
          val lowerNegKeywords = locNegKeywords.map(_.toLowerCase)
          val iterout = iter.map { line =>
            val outKeys = if (locPosAndOr.equalsIgnoreCase("OR")) {
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
        val outRdd = groupingRdd // .filter(l => l.length > 0 && !l.isEmpty)
        val lrdd = outRdd.countByKey()
        val totalLines = accum.value
        val filtered = lrdd.filter { case (k, v) => v >= minCount }
        countedRdd = Map(filtered.toList: _*)
        val duration = ((new Date().getTime - d.getTime) / 100).toInt / 10.0
        loopOut += LoopOutput(totalLines, duration)
        println(s"** RESULT for loop ${nloop + 1}: #recs=$totalLines ${countedRdd.mkString(",")} duration=$duration secs *** ")
        if (!cacheEnabled) {
          rddData = null
        }
      }
      println(s"** Test Completed. Loop durations=${loopOut.map(_.toString).mkString(",")} ** ")
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
        s"""</pre><a href="$url&search_terms=${str.replace(" ",",")}">Details</a><pre>"""
      }
      val useUrl = false
      val formatted = if (useUrl) {
        outcombo.map { case (str, cnt) => s"($cnt) $str ${makeUrl(str)}}" }.mkString("\n")
      } else {
        outcombo.map { case (str, cnt) => s"($cnt) $str" }.mkString("\n")
      }
      val withDuration = s"""Test run time (seconds): ${loopOut.map(_.duration).mkString("\n")}\n\nNumber of records searched: ${loopOut.map(_.nrecs).sum}\n\nMatching Line Results TextFile: $url/results/$exportFile\nMatching Line Results HadoopFile: $saveFile\n\n*** Results ***\n${formatted}"""
      //      val retMapJson = new JSONObject(Map[String, Long](outcombo:_*))
      //      val ret = retMapJson.toString(JSONFormat.defaultFormatter)
      //      ret
      withDuration
    } catch {
      case e: Exception => println(s"Caught ${e.getMessage}"); e.printStackTrace; e.getMessage
    }
  }

}
