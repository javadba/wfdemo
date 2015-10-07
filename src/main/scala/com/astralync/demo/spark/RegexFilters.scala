package com.astralync.demo.spark

import java.util.Date

import com.astralync.demo.TwitterLineParser
import com.astralync.demo.spark.web.DemoHttpServer
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

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

  var sc: SparkContext = _
  def getSc(master: String) = {
    if (sc==null) {
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
      val homeDir = "/home/stephen"
      val master = args(0)
      val dataFile = if (args.length >= 2) args(1) else s"$homeDir/argus/CitiesBaselineCounts2015010520150112.csv"
      val nparts = if (args.length >= 3) args(2).toInt else 100 // assuming 56 workers - do slightly less than 2xworkers
      val nloops = if (args.length >= 4) args(3).toInt else 3
      val cacheEnabled = if (args.length >= 5) args(4).toBoolean else false
      val groupByFields =
        if (args.length >= 6) args(5).split(",").toList else List()
      val minCount = if (args.length >= 7) args(6).toInt else 2
      val posKeywords = args(7).split(",").toList
      val posAndOr = args(8)
      val negKeywords = args(9).split(",").toList
      val negAndOr = args(10)
      val sortBy=args(11)

      println(s"Reading dir=$dataFile using nPartitions=$nparts and caching=$cacheEnabled .. ")
      sc = getSc(master)
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

        val bc = sc.broadcast(posKeywords, posAndOr, negKeywords, negAndOr, TwitterLineParser.headers, groupByFields, DtNames,
          InteractionField, minCount)
        val filtersRdd = rddData.mapPartitionsWithIndex({ case (rx, iter) =>
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

          assert(locPosKeywords.nonEmpty, "Must supply some positive keywords")
          var nlines = 0
          val lowerPosKeywords = locPosKeywords.map(_.toLowerCase)
          val lowerNegKeywords = locNegKeywords.map(_.toLowerCase)
          iter.map { inline =>
            val line = inline.toLowerCase
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
            val allFiltered = if (allGood) {
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
            } else {
              List[(String, Int)]()
            }
            if (nlines % 1000 == 1) {
              println(s"For partition=$rx  number of lines processed=$nlines")
            }
            allFiltered.toList
            //          flattened.flatten
          }
        }, true).flatMap(identity)
        val outRdd = filtersRdd // .filter(l => l.length > 0 && !l.isEmpty)
        val lrdd = outRdd.countByKey()
            val filtered = lrdd.filter { case (k, v) => v >= minCount }
        countedRdd = Map(filtered.toList: _*)
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
      val combo = /* dseq ++ */ cseq
      val outcombo = combo.sortWith { case ((astr, acnt), (bstr, bcnt)) =>
        if (acnt != bcnt) {
          acnt - bcnt >=0
        } else {
          bstr.substring(bstr.indexOf(" ") + 1).compareTo(astr.substring(astr.indexOf(" ") + 1)) >= 0
        }
      }
      val formatted = outcombo.map{ case (s,cnt)  => s"($cnt) $s"}.mkString("\n")
      val withDuration=formatted+s"\n\nDurations: ${dseq.mkString("\n")}"
//      val retMapJson = new JSONObject(Map[String, Long](outcombo:_*))
//      val ret = retMapJson.toString(JSONFormat.defaultFormatter)
//      ret
      withDuration
    } catch {
      case e: Exception => println(s"Caught ${e.getMessage}"); e.printStackTrace; e.getMessage
    }
  }

}
