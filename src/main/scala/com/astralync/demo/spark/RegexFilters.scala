package com.astralync.demo.spark

import java.util.Date

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable
import scala.collection.mutable.{Map => MMap}
import scala.util.parsing.json.JSON
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

  def main(args: Array[String]) = {
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
      System.err.println("""Usage: RegexFilters <master> <datadir> <#partitions> <#loops> <cacheEnabled true/false> <groupByFields separated by commas no spaces>
        e.g. RegexFilters spark://192.168.15.43:7077 hdfs://i386:9000/user/stephen/data 56 3 true posregexFile.json negregexfile.json interaction_created_at,state_province""")
      System.exit(1)
    }
    val homeDir = "/home/stephen"
    val master = args(0)
    val dataFile = if (args.length >= 2) args(1) else s"$homeDir/argus/CitiesBaselineCounts2015010520150112.csv"
    val nparts = if (args.length >= 3) args(2).toInt else 100 // assuming 56 workers - do slightly less than 2xworkers
    val nloops = if (args.length >= 4) args(3).toInt else 3
    val cacheEnabled = if (args.length >= 5) args(4).toBoolean else false
    // GROUP BY FIELDS: will drive the grouping key's!
    val posRegex= if (args.length >= 6) scala.io.Source.fromFile(args(5)).mkString("") else JsonPosRegex
    val negRegex = if (args.length >= 7) scala.io.Source.fromFile(args(6)).mkString("") else JsonNegRegex
    val groupByFields = if (args.length >= 8) args(7).split(",").toList else List()
    var conf = new SparkConf().setAppName("Simple Application").setMaster(master)
    var sc2 = new SparkContext(conf)
    println(s"Connecting to master=${conf.get("spark.master")} reading dir=$dataFile using nPartitions=$nparts and caching=$cacheEnabled .. ")
    println(s"PosRegex=$posRegex\nNegRegex=$negRegex")
    val rddData = sc2.textFile(dataFile, nparts)
    if (cacheEnabled) {
      rddData.cache()
    }

    val durations = mutable.ArrayBuffer[Double]()
    for (nloop <- (0 until nloops)) {
      val d = new Date
      println(s"** Loop #${nloop + 1} starting at $d **")
      val resultMap = MMap[String, Any]()
      val jsonPos = JSON.parseFull(posRegex)
      val jsonPosMap = jsonPos.get.asInstanceOf[Map[String, Any]]
      val jsonNeg = JSON.parseFull(negRegex)
      val jsonNegMap = jsonNeg.get.asInstanceOf[Map[String, Any]]
      val posRegexMap = for ((k, v) <- jsonPosMap) yield {
        (k, v.asInstanceOf[String].r)
      }
      val negRegexMap = for ((k, v) <- jsonNegMap) yield {
        (k, v.asInstanceOf[String].r)
      }
      val bc = sc2.broadcast((posRegexMap, negRegexMap, headers, groupByFields))
      val filtersRdd = rddData.mapPartitions({ iter =>
        val (locPosMap, locNegMap, locHeaders, locGroupingFields) = bc.value
        def parseLine(iline: String, headersList: List[String]):
            Option[Map[String, String]] = {
          val line = iline.trim
          def formatDate(dt: String) = { s"${dt.slice(0,4)}-${dt.slice(8,10)}-${dt.slice(5,7)}" }
          if (line.length == 0 || line.count(c => c == ',') != locHeaders.size) {
            None
          } else {
            val lmap = locHeaders.zip(line.split(",").map(_.replace("\"",""))).toMap
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

        iter.map { line =>
          val posFiltered = for ((k, regex) <- locPosMap) yield {
            val rout = regex.findFirstIn(line) match {
              case Some(l) =>
                val allGood = locNegMap.foldLeft(true) { case (stillgood, (k, negRegex)) =>
                  val rout = negRegex.findFirstIn(l) match {
                    case Some(nl) => false
                    case _ => stillgood
                  }
                  rout
                }
                if (allGood) {
                  val lmap = parseLine(line, locHeaders)
                  if (lmap.isDefined) {
                    val oval = genKey(k, locGroupingFields, lmap.get)
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
          posFiltered.toList.flatten
        }
      }, true)
      val outRdd = filtersRdd.filter(l => l.length > 0 && !l.isEmpty)
      //println(outRdd.take(20).mkString(","))
      //outRdd.take(20).foreach{ l => println(s"len is ${l.length}")}
      val countedRdd = outRdd.flatMap { x => x }.countByKey()
      val duration = ((new Date().getTime - d.getTime) / 100).toInt / 10.0
      durations += duration
      println(s"** RESULT for loop ${nloop + 1}:  ${countedRdd.mkString(",")} duration=$duration secs *** ")
    }
    println(s"** Test Completed. Loop durations=${durations.mkString(",")} ** ")
    if (sc2 != null) {
      sc2.stop()
    }
  }

}
