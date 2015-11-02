package com.astralync.demo.spark.web

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

import java.io.{InputStream, OutputStreamWriter}
import java.net.{URLConnection, URL}

object HttpUtils {
  case class HttpOpts(userAgent: String, encoding: String, httpRequestTimeout: Int = 15000)

  val DefaultHttpOpts = HttpOpts("userAgent", "ISO-8859-1", 15000)
  import collection.JavaConversions._
  import java.net.URLEncoder.encode

  var cookies = Map[String, String]()

  private def loadCookies(conn: URLConnection) {
    for ((name, value) <- cookies) conn.setRequestProperty("Cookie", name + "=" + value)
  }

  private def saveCookies(conn: URLConnection) {
    conn.getHeaderFields.lift("Set-Cookie") match {
      case Some(cList) => cList foreach { c =>
        val (name, value) = c span {
          _ != '='
        }
        cookies += name -> (value drop 1)
      }
      case None =>
    }
  }

  private def encodePostData(data: Map[String, String], httpOpts: HttpOpts) =
    (for ((name, value) <- data) yield encode(name, httpOpts.encoding) + "=" + encode(value, httpOpts.encoding)).mkString("&")

  @inline def readStream(is: InputStream) = scala.io.Source.fromInputStream(is).mkString

  def get(url: String, httpOpts: HttpOpts = DefaultHttpOpts) = {
    val u = new URL(url)
    val conn = u.openConnection()
    conn.setRequestProperty("User-Agent", httpOpts.userAgent)
    conn.setRequestProperty("Content-Type", "text/html")
    conn.setConnectTimeout(httpOpts.httpRequestTimeout)

//    loadCookies(conn)
    conn.connect
//    saveCookies(conn)
    readStream(conn.getInputStream)
  }

  def post(url: String, data: Map[String, String], httpOpts: HttpOpts = DefaultHttpOpts) = {
    val u = new URL(url)
    val conn = u.openConnection

    conn.setRequestProperty("User-Agent", httpOpts.userAgent)
    conn.setRequestProperty("Content-Type", "application/x-www-form-urlencoded")
    conn.setConnectTimeout(httpOpts.httpRequestTimeout)
//    loadCookies(conn)
    conn.setDoOutput(true)
    conn.connect

    val wr = new OutputStreamWriter(conn.getOutputStream())
    wr.write(encodePostData(data, httpOpts))
    wr.flush
    wr.close
//    saveCookies(conn)
    readStream(conn.getInputStream)
  }

}
