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
package com.walmartlabs.mupd8.elasticity

import com.walmartlabs.mupd8.HashRing
import com.walmartlabs.mupd8.messaging.LoadDistInstructMessage
import com.walmartlabs.mupd8.miscM._
import com.walmartlabs.mupd8.messaging.UpdateRingMessage

class ElasticOracle(ring: HashRing, msg: LoadDistInstructMessage) {

  val redistKeys = msg.getDistKeys

  val destHost = msg.getDestHost

  val loadReceiverHost = Integer.parseInt(msg.getDestHost)

  var keySpaces = new Array[Int](redistKeys.length)

  var index = 0
  for (key <- msg.getDistKeys) {
    val keyBytes = key.getBytes()
    keySpaces(index) = ring.getOffset(hash2Double(key))
    index += 1
  }

  def getRedistKeys() = redistKeys

  def isMovingKey(key: Array[Byte]): Boolean = {
    var keySpace = ring.getOffset((hash2Double(key)))
    for (ks <- keySpaces) {
      if (keySpace.equals(ks)) {
        true
      }
    }
    false
  }

  def getLoadReceiverHostIndex() = loadReceiverHost

  def updateRing(): Unit = {
    val msClient = RuntimeProvider.getMessageServerClient()
    val msg = new UpdateRingMessage(redistKeys, destHost)
    msClient.sendMessage(msg)
  }

  def getHostIndex(key: String) = {

  }
}
