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
import com.walmartlabs.mupd8.messaging.Message
import com.walmartlabs.mupd8.messaging.MessageHandler
import com.walmartlabs.mupd8.messaging.NodeFailureMessage
import com.walmartlabs.mupd8.AppStaticInfo
import com.walmartlabs.mupd8.HashRing
import com.walmartlabs.mupd8.Misc._



class BasicMessageHandler(val staticInfo: AppStaticInfo, var ring: HashRing) extends MessageHandler {

  def actOnMessage(msg: Message) {

    def handleFailedNodeMessage(msg: NodeFailureMessage): Unit = {
      val failedHost = msg.getFailedNodeName()
      val index = {
        staticInfo.systemHosts.zipWithIndex.find {
          case (h, i) =>
            getIPAddress(h) == failedHost
        }.get._2
      }
      println("WARN: remove " + failedHost + " with index " + index)
      ring.remove(index)
    }

    val response = msg match {
      case msg: NodeFailureMessage => handleFailedNodeMessage(msg)
      case _ => throw new IllegalStateException(" Unknown message type :" + msg)
    }

    def getStaticInfo() = staticInfo
    def getRing() = ring

  }
}
