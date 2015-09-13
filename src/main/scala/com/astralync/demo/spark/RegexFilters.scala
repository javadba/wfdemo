package com.astralync.demo.spark

import java.util.Date

import com.astralync.demo.spark.web.DemoHttpServer
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import unfiltered.response.ResponseString

import scala.collection.mutable
import scala.collection.mutable.{Map => MMap}
import scala.util.parsing.json._

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

import unfiltered.netty._

/**
 * RegexFilters
 *
 */
object RegexFilters {

  val DefaultPort = 8180
  val DefaultCtx = "/wfdemo"
  var rddData: RDD[String] = _
  var cacheEnabled = true
  var nLoops = 1
  var groupByFields = List[String]()
  val InteractionField = "interaction_created_at"
  val DtNames = s"$InteractionField twitter_user_lang"

  def main(args: Array[String]) = {
    //    submit(args)
    startWebServer
    Thread.currentThread.join
  }

  private var webServer: DemoHttpServer = _

  def startWebServer() = {
    webServer = new DemoHttpServer().run(Map("port" -> DefaultPort, "ctx" -> DefaultCtx))
  }

  //  def startNettyWebServer() = {
  //    import unfiltered.request._
  //    import unfiltered.response._
  //    val hello = unfiltered.netty.cycle.Planify {
  //       case _ => ResponseString("hello world")
  //    }
  //    unfiltered.netty.Server.http(8080).plan(hello).run()
  //  }
  //

  def submit(args: Array[String]) = {
    println(s"Submit entered with ${args.mkString(",")}")
    val DtName = "interaction_created_at"
    // John you need to set this to an input HttpReqParam
    val JsonPosRegex = """{"Party":"(?i-mx:(\\b(party|parties)\\b))",
          "New Years Eve":"(?i-mx:(\\b(new)\\b))",
          "Beer":"(?i-mx:(\\b(beer|drunk|drink)\\b))",
          "Resolutions":"(?i-mx:(\\b(resolution|resolv)\\b))"}"""
    // John you need to set this to an input HttpReqParam
    val JsonNegRegex = """{"Party":"(?i-mx:(\\b(birthday)\\b))",
          "Beer":"(?i-mx:(\\b(bud|budweiser)\\b))"}
                       """

    val headers = List("interaction_created_at", "interaction_content", "interaction_geo_latitude", "interaction_geo_longitude", "interaction_id", "interaction_author_username", "interaction_link", "klout_score", "interaction_author_link", "interaction_author_name", "interaction_source", "salience_content_sentiment", "datasift_stream_id", "twitter_retweeted_id", "twitter_user_created_at", "twitter_user_description", "twitter_user_followers_count", "twitter_user_geo_enabled", "twitter_user_lang", "twitter_user_location", "twitter_user_time_zone", "twitter_user_statuses_count", "twitter_user_friends_count", "state_province")
    val headersOrderMap = (0 until headers.length).zip(headers).toMap
    val headersNameMap = headers.zip(0 until headers.length).toMap

    if (args.length == 0) {
      System.err.println( """Usage: RegexFilters <master> <datadir> <#partitions> <#loops> <cacheEnabled true/false> <groupByFields separated by commas no spaces>
        e.g. RegexFilters spark://192.168.15.43:7077 hdfs://i386:9000/user/stephen/data 56 3 true posregexFile.json negregexfile.json interaction_created_at,state_province""")
      System.exit(1)
    }
    val homeDir = "/home/stephen"
    val master = args(0)
    val dataFile = if (args.length >= 2) args(1) else s"$homeDir/argus/CitiesBaselineCounts2015010520150112.csv"
    val nparts = if (args.length >= 3) args(2).toInt else 100 // assuming 56 workers - do slightly less than 2xworkers
    val nloops = if (args.length >= 4) args(3).toInt else 3
    val cacheEnabled = if (args.length >= 5) args(4).toBoolean else false
//    val posRegex = if (args.length >= 6) scala.io.Source.fromFile(args(5)).mkString("") else JsonPosRegex
    val posKeywords = args(5).split(" ")
//    val negRegex = if (args.length >= 7) scala.io.Source.fromFile(args(6)).mkString("") else JsonNegRegex
    val negKeywords = args(6).split(" ")
    val groupByFields = if (args.length >= 8) args(7).split(",").toList else List()
    val minCount = if (args.length >= 9) args(8).toInt else 2
    var conf = new SparkConf().setAppName("Simple Application").setMaster(master)
    var sc = new SparkContext(conf)
    println(s"Connecting to master=${conf.get("spark.master")} reading dir=$dataFile using nPartitions=$nparts and caching=$cacheEnabled .. ")
    println(s"PosRegex=$posKeywords\nNegRegex=$negKeywords")
    var rddData = sc.textFile(dataFile, nparts)
    if (cacheEnabled) {
      rddData.cache()
    }

    val durations = mutable.ArrayBuffer[Double]()
    var countedRdd: Map[String, Long] = null
    for (nloop <- (0 until nloops)) {
      val d = new Date
      println(s"** Loop #${nloop + 1} starting at $d **")
      val resultMap = MMap[String, Any]()
      val posJson= posKeywords.map { k =>
        s""""$k":"(?i-mx:(\\b($k)\\b))""""
      }.mkString("{",",","}")
      val jsonPos = JSON.parseFull(posJson)
      val posRegexMap = jsonPos.map { m =>
        val p = m.asInstanceOf[Map[String, Any]]
        p.map { case (k, v) =>
          (k, v.asInstanceOf[String].r)
        }
      }
      val negJson= negKeywords.map { k =>
        s""""$k":"(?i-mx:(\\b($k)\\b))""""
      }.mkString("{",",","}")
      val jsonNeg = JSON.parseFull(negJson)
      val negRegexMap = jsonNeg.map { m =>
        m.asInstanceOf[Map[String, Any]].map { case (k, v) =>
          (k, v.asInstanceOf[String].r)
        }
      }

      val bc = sc.broadcast((posRegexMap, negRegexMap, headers, groupByFields, DtNames,
        InteractionField, minCount))
      val filtersRdd = rddData.mapPartitionsWithIndex({ case (rx, iter) =>
        val (locPosMap, locNegMap, locHeaders, locGroupingFields, locDtNames,
        locInteractionField, locMinCount) = bc.value
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
            val nmap = lmap.updated(locInteractionField,
              formatDate(lmap(locInteractionField)))
            Some(nmap)
          }
        }
        def genKey(k: String, groupingFields: Seq[String], lmap: Map[String, String]) = {
          if (groupingFields.isEmpty) {
            k
          } else {
            val groupKey = groupingFields.map { f =>
              //                System.err.println(s"f=$f")
              if (!lmap.contains(f)) {
                System.err.println(s"key $f not found in lmap=${lmap.mkString(",")}")
                k
              } else {
                lmap(f)
              }
            }
            (k +: groupKey).mkString("-")
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
                    //                      println(s"accepting line $line")
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
          if (nlines % 1000 == 1) {
            println(s"For partition=$rx  number of lines processed=$nlines")
          }
          posFiltered.toList.flatten
        }
      }, true)
      val outRdd = filtersRdd.filter(l => l.length > 0 && !l.isEmpty)
      val lrdd = outRdd.flatMap { x => x }.countByKey().filter { case (k, v) => v >= minCount }
      countedRdd = Map(lrdd.toList: _*)
      val duration = ((new Date().getTime - d.getTime) / 100).toInt / 10.0
      durations += duration
      println(s"** RESULT for loop ${nloop + 1}:  ${countedRdd.mkString(",")} duration=$duration secs *** ")
      if (!cacheEnabled) {
        rddData = null
      }
    }
    println(s"** Test Completed. Loop durations=${durations.mkString(",")} ** ")
    val cseq = countedRdd.toSeq
    var dseq = durations.zipWithIndex.map { case (d, x) =>
      (s"Loop$x-Duration", d.toLong)
    }.toSeq
    val retMapJson = new JSONObject(Map[String, Long]((dseq++cseq):_*))
    retMapJson.toString(JSONFormat.defaultFormatter)
  }

}
