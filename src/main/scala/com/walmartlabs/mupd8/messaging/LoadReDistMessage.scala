/**
 * Copyright 2011-2012 @WalmartLabs, a division of Wal-Mart Stores, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package com.walmartlabs.mupd8.messaging
import com.walmartlabs.mupd8.messaging.ActivityStatus._


class LoadReDistMessage(id: Int, transformation: Map[Float, Int], status: ActivityStatus) extends Message(MessageKind.LOAD_REDIST, MessageTransportKind.BROADCAST) {

  def getId() = id
  def getKeySpaceTransformation() = transformation
  def getStatus() = status

  override def toString(): String = {
    var builder = new StringBuilder();
    builder.append(id)
    builder.append("_")
    builder.append("(");
    for ((k, v) <- transformation) {
      builder.append(k)
      builder.append("->")
      builder.append(v)
      builder.append(",")
    }
    if (transformation.size > 0) {
      builder.deleteCharAt(builder.size - 1)
    }
    builder.append(")");
    builder.append("_")
    builder.append(status.toString())
    builder.toString
  }
}

object LoadReDistMessage extends App {
  def initFromString(msgContent: String): LoadReDistMessage = {
    var comp = msgContent.split("_")
    var id = Integer.parseInt(comp(0))
    var trans = Map[Float, Int]()
    var transformation = comp(1).substring(1, comp(1).length() - 1)
    var transComp = transformation.split(",")
    for (t <- transComp) {
      var tComp = t.split("->")
      var k = tComp(0)
      var v = tComp(1)
      trans += (java.lang.Float.parseFloat(k) -> Integer.parseInt(v))
    }
    var status = ActivityStatus.withName(comp(2))
    new LoadReDistMessage(id, trans, status)
  }

  override def main(args: Array[String]): Unit = {
    var msgContent = "0_(0.4->1,0.5->2)_STARTED"
    var obj1 = initFromString(msgContent)
    var obj2Stringify = obj1.toString()
    var obj2 = initFromString(obj2Stringify)
    System.out.println("obj1:" + obj1)
    System.out.println("obj2:" + obj2)
  }
}
