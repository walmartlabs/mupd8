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

object MessageParser extends App {

  def getMessage(msg: String): Message = {
    val tokenizedMessage = msg.substring(msg.indexOf(MessageKind.MessageBegin) + MessageKind.MessageBegin.length()).split(":")
    val messageKind = tokenizedMessage(0).trim()
    val msgContent = tokenizedMessage(1).trim()
    messageKind match {
      case "HOST_REQUEST" => new HostRequestMessage()
      case "NODE_FAILURE" =>
        val failedHost = msgContent
        new NodeFailureMessage(failedHost)
      case "NODE_JOIN" =>
        val joinedHost = msgContent
        new NodeJoinMessage(joinedHost)
      case "LOAD_REDIST" =>
        LoadReDistMessage.initFromString(msgContent)
      case "NODE_STATUS_REPORT" =>
        NodeStatusReportMessage.initFromString(msgContent)
    }

  }
}
