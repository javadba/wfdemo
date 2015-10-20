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

import java.io.InputStream
import java.net.{URLDecoder, InetSocketAddress}
import java.text.Normalizer

import com.astralync.demo.spark.RegexFilters
import com.sun.net.httpserver.{HttpExchange, HttpHandler, HttpServer}
import collection.mutable

class DemoHttpServer {

  def run(map: Map[String,Any]) = {
    new Thread() {
      override def run() {
        val port = map.getOrElse("port", "8180").asInstanceOf[Int]
        val ctx = map.getOrElse("ctx", "/wfdemo").asInstanceOf[String]
        val server = HttpServer.create(new InetSocketAddress(port), 0)
        server.createContext(ctx, new RootHandler())
        server.setExecutor(null)

        server.start()
        Thread.currentThread.join
      }
    }.start
    this
  }

}

class RootHandler extends HttpHandler {

  def handle(t: HttpExchange) {
//    val res = process(t.getRequestBody)
    val rparams = t.getRequestURI.getQuery
    val params = if (rparams!=null && rparams.nonEmpty) rparams.split("&")
          else HttpUtils.readStream(t.getRequestBody).split("&")
    val pmap = params.map(_.split("=")).map{ a => (a(0),a(1))}.toMap
    val res = process(t, pmap)
    sendResponse(t, res)
  }

  val MaxPrint = 256*1024
//  private def process(body: InputStream) = {
//    val strm =  scala.io.Source.fromInputStream(body)
//    val cmd = strm.mkString
//    System.err.println(s"Received [$cmd]")
  private def process(t: HttpExchange, params: Map[String,String]) = {
    val eparams = params.mapValues(pv=> URLDecoder.decode(pv))
    var cmdline = mutable.ArrayBuffer(eparams("cmdline").split(" "):_*).map(_.trim).filter(_.length > 0)
    val query = t.getRequestURI.getRawQuery
//    val query = t.getRequestURI.getRawQuery
    cmdline ++= Array(eparams("sortBy"),
       eparams("saveFile"), eparams("exportFile"), /* eparams("jsonPos"),eparams("jsonNeg"), */
      s"${t.getRequestURI.toString}",
      if (eparams.contains("searchTerms")) { eparams("searchTerms") }
        else {"NONE"}
    )
    System.err.println(s"Received cmdline=[${cmdline.mkString(" ")}]  eparams=[${eparams.mkString(",")}]")
    val res = RegexFilters.submit(cmdline.toArray)
    System.err.println(s"Result: ${res.substring(0,math.min(MaxPrint, res.length))}")
    res
  }

  private def sendResponse(t: HttpExchange, resp: String) {
    t.sendResponseHeaders(200, resp.length())
    val os = t.getResponseBody
//    val outb = resp.getBytes("UTF-8")
    val nout = Normalizer.normalize(resp, Normalizer.Form.NFD);
    val repout = nout.replaceAll("[^\\x00-\\x7F]", "")
    val repoutb = repout.getBytes
    os.write(repoutb)
    os.close()
  }

}
