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

import com.walmartlabs.mupd8.elasticity.TransporterKind._
import com.walmartlabs.mupd8.AppRuntime
import com.walmartlabs.mupd8.MessageServerClient
import com.walmartlabs.mupd8.AppStaticInfo
import com.walmartlabs.mupd8.application.statistics.StatisticsBootstrap
import com.walmartlabs.mupd8.application.statistics.NodeStatisticsCollector
import com.walmartlabs.mupd8.application.statistics.PrePerformer
import java.util.ArrayList
import scala.collection.mutable.MutableList

object RuntimeProvider {

  var loadPlanner: LoadPlanner = null;
  var appRuntime: AppRuntime = null;
  var messageClient: MessageServerClient = null
  var initialized: Boolean = false
  var appStaticInfo: AppStaticInfo = null
  var performerStatsCollectors: java.util.List[PrePerformer] = new java.util.ArrayList[PrePerformer]

  def getMupd8Runtime(appID: Int,
    poolsize: Int,
    app: AppStaticInfo,
    collectStatistics: Boolean = false, elastic: Boolean = false, useNullPool: Boolean = false): AppRuntime = {

    def init(appRuntime: AppRuntime) = {
      messageClient = appRuntime.getMessageServerClient();
      initialized = true
      loadPlanner = new KeyDistributionPlanner(appRuntime)
      appStaticInfo = app

      if (collectStatistics || elastic) {
        StatisticsBootstrap.INSTANCE.bootstrap()
      }
      if (elastic) {
        val messenger = new NodeStatisticsMessenger()
        val messengerThread = new Thread(messenger)
        messengerThread.start();
      }
    }

    if (appRuntime == null) {
      elastic match {
        case false =>
          appRuntime = new AppRuntime(appID, poolsize, app, useNullPool)
        case true =>
          appRuntime = new ElasticAppRuntime(appID, poolsize, app, useNullPool);
      }
      init(appRuntime)
      appRuntime.getMessageHandler().initialize()
      appRuntime
      
    } else {
      appRuntime
    }

  }

  def getLoadTransporter(transporterKind: TransporterKind): LoadTransporter = {
    transporterKind match {
      case TransporterKind.FLUSH_RELOAD_TRANSPORTER => new FlushReloadTransporter;
      case TransporterKind.IN_MEMORY_TRANSPORTER => new InMemoryLoadTransporter;
      case _ => throw new IllegalStateException(" Unknoen transporter kind :" + transporterKind)
    }
  }

  def getMessageServerClient(): MessageServerClient = if (initialized) { messageClient } else { throw new IllegalStateException(" Runtime not initialized") }

  def getStatisticsCollector(): NodeStatisticsCollector = NodeStatisticsCollector.INSTANCE

  def getLoadPlanner(): LoadPlanner = if (initialized) { loadPlanner } else { throw new IllegalStateException(" Runtime not initialized") }

  def isPlannerHost(): Boolean = { if (initialized) { (appRuntime.getAppStaticInfo().isPlannerHost()) } else { throw new IllegalStateException(" Runtime not initialized") } }

  def getPlannerHost(): String = { return appStaticInfo.plannerHost }

  def registerPrePerformer(prePerformer: PrePerformer): Unit = {
    performerStatsCollectors.add(prePerformer)
  }

  def getPrePerformers(): java.util.List[PrePerformer] = performerStatsCollectors

}

