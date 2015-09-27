package com.astralync.demo


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
 * ParserTree
 *
 */

import ParserTree._

object ParserTree {
  type StrBoolNode = Node[String, Boolean]
  type StrBoolBinNode = BinNode[String, Boolean]
  type StrBoolMonoNode = MonoNode[String, Boolean]

  sealed abstract class MonoOp[A](val name: String, val token: String, val op: A => A) {
    def apply(a: A) = op(a)
  }

  case object Not extends MonoOp[Boolean]("NOT", "NOT", x => !x)

  case object And extends BinOp[Boolean]("AND", "AND", (a, b) => a && b)

  case object Or extends BinOp[Boolean]("OR", "OR", (a, b) => a || b)

  val binPreds = Seq(And, Or).map(_.name)
  val monoPreds = Seq(Not).map(_.name)

  abstract class BinOp[A](val name: String, val token: String, val op: (A, A) => A) {
    def apply(a: A, b: A): A = op(a, b)
  }

  abstract class Tree[D, A] {
    val root: Node[D, A]

    def compute(d: D) = root.compute(d)
  }

  trait Processor[D, A] {
    def compute(d: D): A
  }

  type StrBoolProcessor = Processor[String, Boolean]

  abstract class Node[D, A](var parent: Option[BinNode[D, A]], var optMonoOp: Option[MonoOp[A]] = None) {
    def compute(d: D): A

    //  = processor.compute(d)
    def apply(d: D) = compute(d)
  }

  case class BinNode[D, A](par: Option[BinNode[D, A]], left: Node[D, A], right: Node[D, A], binOp: BinOp[A],
    ooptMonoOp: Option[MonoOp[A]] = None)
    extends Node[D, A](par, ooptMonoOp) {
    override def compute(d: D): A = {
      val raw = binOp(left(d), right(d))
      //      val raw = binOp(left.apply(), right.apply())
      if (optMonoOp.isDefined) {
        optMonoOp.get(raw)
      } else {
        raw
      }
    }
  }

  //  object BinNode {
  //    import reflect.runtime.universe._
  //    def rotate[D: TypeTag,A: TypeTag](grandpa: BinNode[D,A], rightChild: Node[D,A], newNode: BinNode[D,A]) = {
  //      grandpa.right = newNode
  //      val right = if (!rightChild.isInstanceOf[BinNode[_,_]]) {
  //        val newRight = new StrBoolBinNode(grandpa, )
  //
  //      rightChild.parent = newNode
  //      newNode.left = rightChild
  //      newNode.parent = grandpa
  //
  //    }
  //  }

  case class MonoNode[D, A](par: Option[BinNode[D, A]], processsor: Processor[D, A], nval: D, ooptMonoOp: Option[MonoOp[A]] = None)
    extends Node[D, A](par, ooptMonoOp) {
    override def compute(d: D): A = {
      val raw = processsor.compute(d)
      if (optMonoOp.isDefined) {
        optMonoOp.get(raw)
      } else {
        raw
      }
    }
  }

}
