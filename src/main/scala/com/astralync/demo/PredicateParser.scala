package com.astralync.demo

import com.astralync.demo.ParserTree.{Not, StrBoolBinNode, StrBoolNode, BinOp}

import scala.util.matching.Regex

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
 * PredicateParser
 *
 */
import ParserTree._
object PredicateParser {

  val delims = " ()\"'"

  def pr(msg: String) = println(msg)

  def parse(toks: Array[String]): StrBoolNode  = {
//    var root: StrBoolNode = new TwitterKeywordProcessor()
//    val root: StrBoolNode = null
//    def parse0(toks: Array[String], tx: Int): (StrBoolNode, Int) = {
//      var itx = tx
//      var localDone = false
//
//      def getMonoNode(toks: Array[String], tx: Int) = {
//        var itx = tx
//        val monop = if (monoPreds.contains(toks(itx))) {
//          toks(itx) match {
//            case "NOT" => Some(Not)
//            case _ => throw new IllegalArgumentException(s"Unknown Mono predicate $monop")
//          }
//          itx +=1
//        } else {
//          None
//        }
//        val keyword = toks(itx); itx += 1
//        (new StrBoolMonoNode(new TwitterKeywordProcessor(keyword), keyword, monop),itx)
//      }
////      var curMonoNode: StrBoolMonoNode = null
////      var curBinNode: StrBoolBinNode = null
//      case class PState(var curNode: Option[StrBoolNode] = null,
//          var binPred: Option[String] = null)
//      val st = PState()
//      def addNode(st: PState, newNode: StrBoolNode) = {
//        if (st.binPred
//      while (itx < toks.length-1 && !localDone) {
//        val tok = toks(itx)
//        itx += 1
//        tok match {
//          case "(" =>
//            val (newNode, newx) = parse0(toks, itx)
//            itx = newx
//            st.curNode = addNode(st,newNode)
//          case ")" => localDone = true
//          case s if binPreds.contains(s) =>
//            if (curNode == null) {
//              throw new IllegalStateException(s"Binary pred encountered without prior Node available: $s")
//            } else {
//              if (binPred == Or.name && s == And.name) {
//                val (andNode, newerx) = parse0(toks, itx)
//                itx = newerx
//                curNode = new StrBoolBinNode(curNode, andNode, Or)
//              } else {
//                val (monoNode,newerx) = getMonoNode(toks, itx)
//                itx = newerx
//              }
//            }
//          case s if monoPreds.contains(s) => monoPred = s
//          case s if curNode.isInstanceOf[ =>
//            if curNod
     null
  }
  def tokenize(in: String) = {
    val inds = in.toCharArray.zipWithIndex.filter { x => delims.contains(x._1) }.map(_._2)
    var prevx = 0
    val toks = inds.map { ind =>
      val ss = if (ind > 0) {
        Seq(in.substring(prevx, ind), in(ind))
      } else {
       Seq(in(0))
      }
      prevx = ind+1
      ss
    }.flatten.map(_.toString.trim).filter(_.length>0)
//    pr(toks.mkString(","))
    import collection.mutable
    val outToks = mutable.ArrayBuffer[String]()
    var cum:String = null
    var cumTok:String = null
    for (tok  <- toks) {
      if ("\"'".contains(tok)) {
        if (tok==cumTok) {
            outToks += cum
            cum = null
            cumTok = null
        } else if (cum!=null) {
          cum += tok
        } else {
          cum = ""
          cumTok = tok
        }
      } else if (cum != null) {
        if (cum.length > 0) {
          cum += " "
        }
        cum += tok
      } else {
        outToks += tok
      }
//      pr(s"cum=$cum tok=$tok cumTok=$cumTok")
    }
    pr(outToks.mkString(","))
    outToks
  }


  def main(args: Array[String]) = {
    val toks = tokenize(s""" hello " double ' quote " where (paren1) between (paren2) after 'single quote' """)
    val out = parse(toks.toArray)
  }

}
