package com.astralync.demo

import com.astralync.demo.ParserTree.StrBoolProcessor

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

class TwitterKeywordProcessor(val keyword: String) extends StrBoolProcessor {
//    val lmap = TwitterLineParser.parse(line)
  override def compute(line: String): Boolean = {
    line.toLowerCase.contains(keyword.toLowerCase)
  }
}

/**
 * TwitterLineParser
 *
 */
object TwitterLineParser {
  val InteractionField = "interaction_created_at"
  val headers = List(InteractionField, "interaction_content", "interaction_geo_latitude", "interaction_geo_longitude", "interaction_id", "interaction_author_username", "interaction_link", "klout_score", "interaction_author_link", "interaction_author_name", "interaction_source", "salience_content_sentiment", "datasift_stream_id", "twitter_retweeted_id", "twitter_user_created_at", "twitter_user_description", "twitter_user_followers_count", "twitter_user_geo_enabled", "twitter_user_lang", "twitter_user_location", "twitter_user_time_zone", "twitter_user_statuses_count", "twitter_user_friends_count", "state_province")
  val headersOrderMap = (0 until headers.length).zip(headers).toMap
  val headersNameMap = headers.zip(0 until headers.length).toMap

  def parse(iline: String): Option[Map[String, String]] = {
    val line = iline.trim
    def formatDate(dt: String) = {
      s"${dt.slice(0, 4)}-${dt.slice(8, 10)}-${dt.slice(5, 7)}"
    }
    if (line.length == 0 || line.count(c => c == ',') != headers.size) {
      None
    } else {
      val lmap = headers.zip(line.split(",").map(_.replace("\"", ""))).toMap
      val nmap = lmap.updated(InteractionField, formatDate(lmap(InteractionField)))
      Some(nmap)
    }
  }
}
