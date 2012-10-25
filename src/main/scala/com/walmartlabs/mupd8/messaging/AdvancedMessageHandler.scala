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
import com.walmartlabs.mupd8.messaging.ActivityStatus
import com.walmartlabs.mupd8.elasticity.ElasticWrapper
import com.walmartlabs.mupd8.Performer
import com.walmartlabs.mupd8.elasticity.TransporterKind
import com.walmartlabs.mupd8.PerformerPacket
import com.walmartlabs.mupd8.messaging.ActivityStatus
import com.walmartlabs.mupd8.messaging.ActivityStatus._
import com.walmartlabs.mupd8.application.statistics.NodeStatisticsCollector
import com.walmartlabs.mupd8.Misc._

class AdvancedMessageHandler(val staticInfo: AppStaticInfo, var ring: HashRing, val appRuntime: ElasticAppRuntime) extends MessageHandler {

  val elasticMapUpdatePool: ElasticMapUpdatePool[PerformerPacket] = appRuntime.getMapUpdatePool().asInstanceOf[ElasticMapUpdatePool[PerformerPacket]]
  var performers: List[Performer] = Nil
  val loadReDistMsgInbox = new LinkedBlockingQueue[Message]
  val loadReDistHandler = new LoadDistributionExecutor(loadReDistMsgInbox, staticInfo, appRuntime, ring, this, elasticMapUpdatePool)
  var oracle: ElasticOracle = null
  var successfulDistributionCount = 0

  def actOnMessage(message: Message): Unit = {

    def handleFailedNodeMessage(msg: NodeFailureMessage): Unit = {
       System.out.println("A REMOVING HOST:" + msg.getFailedNodeName())
     
      val failedHost = msg.getFailedNodeName()
      val index = {
        staticInfo.systemHosts.zipWithIndex.find {
          case (h, i) =>
            getIPAddress(h) == failedHost
        }.get._2
      }
      println("WARN: remove " + failedHost + " with index " + index)
      ring.remove(index)
      staticInfo.removeHost(index)
    }

    def handleLoadReDistMessage(msg: LoadReDistMessage): Unit = {
      loadReDistMsgInbox.put(msg)
    }

    def handleNodeJoinMessage(msg: NodeJoinMessage): Unit = {
      System.out.println(" Welcome :" + msg.getJoiningNode())
    }

    def handleNodeStatusReportMessage(msg: NodeStatusReportMessage): Unit = {
      if (RuntimeProvider.isPlannerHost()) {
        RuntimeProvider.getLoadPlanner().receiveNodeStatisticsReport(msg.getNodeStatisticsReport)
      } else {
        System.out.println(" discarding node status report! as self not being planner")
      }
    }

    val response = message match {
      case msg: NodeFailureMessage => handleFailedNodeMessage(msg)
      case msg: NodeJoinMessage => handleNodeJoinMessage(msg)
      case msg: LoadReDistMessage => handleLoadReDistMessage(msg)
      case msg: NodeStatusReportMessage => handleNodeStatusReportMessage(msg)
      case _ => System.out.println(" Unknown message kind: " + message.getKind())
    }
  }

  def registerPerformer(performer: Performer): Unit = {
    performers ::= performer
  }

  def notifyPerformersBeginLoadDistribution() = {
    for (performer <- performers) {
      performer.asInstanceOf[ElasticWrapper].setLoadRedistributionInProgress(oracle)
    }
  }

  def notifyRegisteredPerformersCompleteLoadDistribution() = {
    for (performer <- performers) {
      performer.asInstanceOf[ElasticWrapper].setLoadRedistributionInProgress(oracle)
    }
  }

}

class LoadDistributionExecutor(messageQueue: LinkedBlockingQueue[Message], staticInfo: AppStaticInfo, appRuntime: AppRuntime, ring: HashRing,
  messageHandler: AdvancedMessageHandler, elasticMapUpdatePool: ElasticMapUpdatePool[PerformerPacket]) extends Thread {

  var successfulDistributionCount = 0
  var loadRedistributionStatus: ActivityStatus = ActivityStatus.SUCCESS

  override def run(): Unit = {
    var msgClient = RuntimeProvider.getMessageServerClient()
    while (true) {
      val message = messageQueue.take();
      message match {
        case msg: LoadReDistMessage =>
          val transforamtions = msg.getKeySpaceTransformation()
          var status = msg.getStatus()
          status match {
            case ActivityStatus.STARTED =>
              if (loadRedistributionStatus.equals(ActivityStatus.STARTED)) {
                throw new IllegalStateException(" Load re-distribution alread in progress");
              }
              loadRedistributionStatus = ActivityStatus.STARTED
              var oracle = new ElasticOracle(ring, msg)
              messageHandler.notifyPerformersBeginLoadDistribution() // after this statement, all performers have initiated buffering input
              elasticMapUpdatePool.notifyLoadRedistributionBegin(oracle) // after this, MapUpdate pool begins to buffer output 
              try { // now begin transporting
                val transporter = RuntimeProvider.getLoadTransporter(TransporterKind.IN_MEMORY_TRANSPORTER);
                //TODO: Have below 3 statements handled in a diff thread
                transporter.transferSlates(appRuntime, oracle) // after this, slates have been transferred 
                oracle.updateRing()
                elasticMapUpdatePool.notifyLoadRedistributionCompletion() // ship output queue
                val loadDstCompletion = new LoadReDistResponseMessage(msg.getId(), msg.getKeySpaceTransformation(), "localhost", ActivityStatus.SUCCESS)
                msgClient.sendMessage(loadDstCompletion)
              } catch {
                case e: Exception =>
                  RuntimeProvider.getLoadPlanner().notifyLoadRedistributionActivityStatus(ActivityStatus.FAILED)
                  System.out.println(" Aborted load redistribution ")
                  val loadDstCompletion = new LoadReDistResponseMessage(msg.getId(), msg.getKeySpaceTransformation(), "localhost", ActivityStatus.FAILED)
                  msgClient.sendMessage(loadDstCompletion)
              }
          }

        case m: LoadReDistResponseMessage =>
          var status = m.getStatus()
          status match {
            case ActivityStatus.SUCCESS =>
              successfulDistributionCount += 1
              if (successfulDistributionCount == staticInfo.systemHosts.length) {
                elasticMapUpdatePool.notifyLoadRedistributionCompletion()
                RuntimeProvider.getLoadPlanner().notifyLoadRedistributionActivityStatus(ActivityStatus.SUCCESS);
              }
            case ActivityStatus.FAILED =>
              if (successfulDistributionCount == staticInfo.systemHosts.length) {
                elasticMapUpdatePool.notifyLoadRedistributionCompletion()
                loadRedistributionStatus = ActivityStatus.FAILED
                RuntimeProvider.getLoadPlanner().notifyLoadRedistributionActivityStatus(ActivityStatus.FAILED);
              }
          }

      }

    }
  }
}
