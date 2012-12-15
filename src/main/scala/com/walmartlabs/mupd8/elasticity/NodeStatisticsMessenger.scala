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

import com.walmartlabs.mupd8.messaging.NodeStatusReportMessage
import com.walmartlabs.mupd8.application.statistics.NodeStatisticsReport
import com.walmartlabs.mupd8.application.statistics.PerformerStatisticsCollector
import com.walmartlabs.mupd8.application.statistics.PrePerformer
import com.walmartlabs.mupd8.application.statistics.PerformerStatisticsCollector

class NodeStatisticsMessenger extends Runnable {

  val nodeStatisticsCollector = RuntimeProvider.getStatisticsCollector()
  val performerStatsCollectors = RuntimeProvider.getPrePerformers()
  val msClient = RuntimeProvider.getMessageServerClient()

  def run() = {
    var hotKeys: java.util.List[String] = new java.util.ArrayList[String]
    while (true) {
      hotKeys.clear();
      val iterator = performerStatsCollectors.iterator()
      while (iterator.hasNext()) {
        val statCollector = iterator.next()
        val keys = (statCollector.asInstanceOf[com.walmartlabs.mupd8.application.statistics.PerformerStatisticsCollector]).getHotKeys()
        for (key <- keys) {
          hotKeys.add(key)
        }
      }
      val k = new Array[String](hotKeys.size())
      val it = hotKeys.iterator()
      var i =0
      while(it.hasNext()){
        k(i) = it.next()
      }
      var statsReport = new NodeStatisticsReport("localhost", System.currentTimeMillis(), java.lang.Double.parseDouble(nodeStatisticsCollector.getNodeLoadAvg()),k)
      var nodeStatusMessage = new NodeStatusReportMessage(RuntimeProvider.getPlannerHost(), statsReport)
      msClient.sendMessage(nodeStatusMessage)
      Thread.sleep(1000 * 5)
    }
  }
}

