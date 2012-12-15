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
import com.walmartlabs.mupd8.elasticity.ElasticOracle
import com.walmartlabs.mupd8.HashRing
import com.walmartlabs.mupd8.elasticity.ElasticMapUpdatePool
import java.util.concurrent.LinkedBlockingQueue
import com.walmartlabs.mupd8.AppStaticInfo
import com.walmartlabs.mupd8.AppRuntime
import com.walmartlabs.mupd8.elasticity.RuntimeProvider
import com.walmartlabs.mupd8.messaging.ActivityStatus._
import com.walmartlabs.mupd8.elasticity.TransporterKind
import com.walmartlabs.mupd8.PerformerPacket

class LoadDistSendSideExecutor(messageQueue: LinkedBlockingQueue[Message], staticInfo: AppStaticInfo, appRuntime: AppRuntime, ring: HashRing,
  messageHandler: AdvancedMessageHandler, elasticMapUpdatePool: ElasticMapUpdatePool[PerformerPacket]) extends Thread {

  var successfulDistributionCount = 0
  var loadRedistributionStatus: ActivityStatus = ActivityStatus.SUCCESS

  def getMessageQueue() = messageQueue

  override def run(): Unit = {
    var msgClient = RuntimeProvider.getMessageServerClient()
    while (true) {
      val message = messageQueue.take();
      message match {
        case msg: LoadDistInstructMessage =>
          val redistKeys = msg.getDistKeys
          val oracle = new ElasticOracle(ring, msg)
          try {
            messageHandler.notifyPerformersBeginLoadDist() // after this statement, all performers have initiated buffering input
            val transporter = RuntimeProvider.getLoadTransporter(TransporterKind.FLUSH_RELOAD_TRANSPORTER);
            transporter.transferSlates(oracle) // after this, slates have been transferred 
            oracle.updateRing()
            messageHandler.notifyPerformersLoadDistStateTransferred()
          } catch {
            case e: Exception =>
              RuntimeProvider.getLoadPlanner().notifyLoadRedistributionActivityStatus(ActivityStatus.FAILED)
              System.out.println(" Aborted load redistribution ")
          }
      }
    }
  }
  
  
}