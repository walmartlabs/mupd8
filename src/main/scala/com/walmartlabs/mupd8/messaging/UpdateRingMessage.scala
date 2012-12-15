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

class UpdateRingMessage(distKeys: Array[String], destHost: String) extends Message(MessageKind.LOAD_DIST_INSTRUCT, MessageTransportKind.UNICAST) {
  def getDestHost = destHost
  def getDistKeys = distKeys

  override def toString(): String = {
    var buffer: StringBuilder = new StringBuilder("" + destHost + ",")
    for (key <- distKeys) {
      buffer.append(key)
      buffer.append("|")
    }
    buffer.toString()
  }
}

object UpdateRingMessage {

  def initFromString(msgContent: String): UpdateRingMessage = {
    val tokens = msgContent.split(",")
    val dest = tokens(0)
    val keys = tokens(1).substring(1, tokens(1).length() - 1).split("|")
    new UpdateRingMessage(keys, dest)
  }
}

