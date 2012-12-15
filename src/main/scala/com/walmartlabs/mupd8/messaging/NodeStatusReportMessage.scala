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
import com.walmartlabs.mupd8.application.statistics.NodeStatisticsReport

class NodeStatusReportMessage(recipient: String, nodeReport: NodeStatisticsReport) extends Message(MessageKind.NODE_STATUS_REPORT, MessageTransportKind.UNICAST) {

  def getNodeStatisticsReport = nodeReport
  def getRecipient = recipient
  override def toString(): String = {
    recipient + "_" + nodeReport.toString()
  }

}

object NodeStatusReportMessage {

  def initFromString(msgContent: String) : NodeStatusReportMessage = {
    var recipient = msgContent.substring(0, msgContent.indexOf("_"))
    var reportContent = msgContent.substring(recipient.length()+1)
    new NodeStatusReportMessage(recipient, NodeStatisticsReport.initFromString(msgContent))
  }
}
