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
import java.util.concurrent.LinkedBlockingQueue
import com.walmartlabs.mupd8.elasticity.ElasticOracle
import com.walmartlabs.mupd8.HashRing
import com.walmartlabs.mupd8.elasticity.ElasticAppRuntime
import com.walmartlabs.mupd8.elasticity.ElasticMapUpdatePool
import com.walmartlabs.mupd8.AppStaticInfo
import com.walmartlabs.mupd8.AppRuntime
import com.walmartlabs.mupd8.elasticity.RuntimeProvider
import com.walmartlabs.mupd8.elasticity.ElasticWrapper
import com.walmartlabs.mupd8.Performer
import com.walmartlabs.mupd8.elasticity.TransporterKind
import com.walmartlabs.mupd8.PerformerPacket
import com.walmartlabs.mupd8.messaging.ActivityStatus._
import com.walmartlabs.mupd8.application.statistics.NodeStatisticsCollector
import com.walmartlabs.mupd8.Misc._
import com.walmartlabs.mupd8.Mupd8Utils
import com.walmartlabs.mupd8.Mupd8Main

class AdvancedMessageHandler(val staticInfo: AppStaticInfo, val appRuntime: ElasticAppRuntime) extends MessageHandler {

  var ring: HashRing = null
  val elasticMapUpdatePool: ElasticMapUpdatePool[PerformerPacket] = appRuntime.getMapUpdatePool().asInstanceOf[ElasticMapUpdatePool[PerformerPacket]]
  var performers: List[Performer] = Nil
  val loadDistSendSideMsgInbox = new LinkedBlockingQueue[Message]
  val loadReDistSendSideHandler = new LoadDistSendSideExecutor(loadDistSendSideMsgInbox, staticInfo, appRuntime, ring, this, elasticMapUpdatePool)

  var oracle: ElasticOracle = null
  var successfulDistributionCount = 0

  def getSendSideHandler() = loadReDistSendSideHandler

  override def initialize(ring:HashRing) = {
    loadReDistSendSideHandler.start()
    this.ring=ring
  }

  def actOnMessage(message: Message): Unit = {

    def handleFailedNodeMessage(msg: NodeFailureMessage): Unit = {
      val failedHost = msg.getFailedNodeName()
      val failedHostIpAddress = getIPAddress(failedHost)
      var hostIndex = -1
      val indexed = staticInfo.systemHosts.zipWithIndex
      for(host <- indexed){
         var ipAddress = getIPAddress(host._1)
         if(ipAddress == failedHostIpAddress){
           hostIndex=host._2
         }
      }
      if(hostIndex != -1){
        println("WARN: remove " + failedHost + " with index " + hostIndex)
        if(ring != null){
          ring.remove(hostIndex)
        }
        staticInfo.removeHost(hostIndex)
      } 
    }

    def handleHostListMessage(msg: HostListMessage): Unit = {
      println(" HANDLING HOST LIST RESPONSE MESSAGE")
      staticInfo.setSystemHosts(msg.getHostList())
    }

    def handleNodeJoinMessage(msg: NodeJoinMessage): Unit = {
      // not currently implemented
    }

    def handleNodeStatusReportMessage(msg: NodeStatusReportMessage): Unit = {
      if (RuntimeProvider.isPlannerHost()) {
        RuntimeProvider.getLoadPlanner().receiveNodeStatisticsReport(msg.getNodeStatisticsReport)
      }
    }

    def handleLoadDistInstructMessage(msg: LoadDistInstructMessage): Unit = {
      if (isLocalHost(msg.getSourceHost)) { //send side
        loadReDistSendSideHandler.getMessageQueue().offer(msg)
      }
    }

    def handleUpdateRingMessage(msg: UpdateRingMessage): Unit = synchronized {
      val redistKeys = msg.getDistKeys
      val destHostIndex = Mupd8Utils.getHostNameToIndex(RuntimeProvider.appStaticInfo, msg.getDestHost)
      for (key <- redistKeys) {
        ring.modifyAssignment(ring.getOffset(Mupd8Utils.hash2Float(key)), destHostIndex)
      }
    }

     message match {
      case msg: NodeFailureMessage => handleFailedNodeMessage(msg)
      case msg: NodeJoinMessage => handleNodeJoinMessage(msg)
      case msg: HostListMessage => handleHostListMessage(msg)
      case msg: NodeStatusReportMessage => handleNodeStatusReportMessage(msg)
      case msg: LoadDistInstructMessage => handleLoadDistInstructMessage(msg)
      case msg: UpdateRingMessage => handleUpdateRingMessage(msg)
      case _ => System.out.println(" Unknown message kind! " + message.getKind())
    }
  }

  def registerPerformer(performer: Performer): Unit = {
    performers ::= performer
  }

  def notifyPerformersBeginLoadDist() = {
    for (performer <- performers) {
      performer.asInstanceOf[ElasticWrapper].setLoadRedistributionInProgress(oracle)
    }
  }

  def notifyPerformersLoadDistStateTransferred() = {
    for (performer <- performers) {
      performer.asInstanceOf[ElasticWrapper].setLoadRedistStateTransferCompleted()
    }
  }


}

