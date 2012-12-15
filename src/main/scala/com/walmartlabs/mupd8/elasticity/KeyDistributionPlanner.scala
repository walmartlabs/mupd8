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

import com.walmartlabs.mupd8.AppRuntime
import com.walmartlabs.mupd8.application.statistics.NodeStatisticsReport
import com.walmartlabs.mupd8.messaging.ActivityStatus._
import com.walmartlabs.mupd8.Mupd8Utils
import com.walmartlabs.mupd8.messaging.LoadDistInstructMessage
import scala.collection.mutable.Queue
import java.util.ArrayList
import java.util.Random

class KeyDistributionPlanner(val appRuntime: AppRuntime) extends LoadPlanner {

  val maxReports = 5
  val nodeReports = collection.mutable.Map.empty[String, collection.mutable.ListBuffer[NodeStatisticsReport]]
  val overloadedHosts = new collection.mutable.ListBuffer[String]
  var redistributionInProgress: Boolean = false
  var redistRequests = new Queue[NodeStatisticsReport]

  def receiveNodeStatisticsReport(report: NodeStatisticsReport): Unit = synchronized {
    val host = report.getHostname()
    val reportList = nodeReports.getOrElse(host, new collection.mutable.ListBuffer[NodeStatisticsReport])
    if (reportList.isEmpty) {
      nodeReports += (host -> reportList)
    } else if (reportList.length == maxReports) {
      reportList.remove(0)
    }
    reportList += report
    val redistKeys = assessNeedToRedistributeLoad(report)
    if (redistKeys != null) {
      initiateLoadRedistribution(report)
    }
  }

  def assessNeedToRedistributeLoad(report: NodeStatisticsReport): Array[String] = {
    val redistKeys = report.getRedistKeys()
    val numHosts = RuntimeProvider.appStaticInfo.getSystemHosts().length
    if (numHosts > 1 && redistKeys != null && redistKeys.length > 0) {
      if (redistributionInProgress == false) {
        redistKeys
      } else {
        redistRequests.enqueue(report)
        null
      }
    }
    null
  }

  def initiateLoadRedistribution(report: NodeStatisticsReport) {
    val redistKeys = report.getRedistKeys
    println("Initiating redistribution of keys")
    val fl = Mupd8Utils.hash2Float(redistKeys(0).getBytes())
    val sourceHostIndex = appRuntime.getHashRing().apply(fl)
    var destHostIndex = getDestHostIndex(report)
    val sourceHost = appRuntime.getAppStaticInfo().getSystemHosts()(sourceHostIndex)
    val destHost = appRuntime.getAppStaticInfo().getSystemHosts()(destHostIndex)
    val loadDistMesg = new LoadDistInstructMessage(0, sourceHost, destHost, redistKeys)
    appRuntime.getMessageServerClient().sendMessage(loadDistMesg)
    redistributionInProgress = true
  }

  def getDestHostIndex(report: NodeStatisticsReport): Int = {
    val sourceHostIndex = Mupd8Utils.getHostNameToIndex(RuntimeProvider.appStaticInfo, report.getHostname())
    val systemHosts = RuntimeProvider.appStaticInfo.getSystemHosts()
    val candidateHosts = new ArrayList[String]
    for (host <- systemHosts) {
      if (!(host.equals(report.getHostname()))) {
        candidateHosts.add(host)
      }
    }
    if (candidateHosts.isEmpty()) {
      -1
    } else {
      val rand = new Random()
      val index = rand.nextInt(candidateHosts.size())
      val candidateHost = candidateHosts.get(index)
      val destinationHostIndex = Mupd8Utils.getHostNameToIndex(RuntimeProvider.appStaticInfo, candidateHost)
      destinationHostIndex
    }
  }

  def notifyLoadRedistributionActivityStatus(status: ActivityStatus): Unit = synchronized {
    status match {
      case SUCCESS => initiatePendingReDistribution()
      case FAILED => initiatePendingReDistribution()
    }
  }

  private def initiatePendingReDistribution(): Unit = {
    if (redistRequests.length > 0) {
      var nodeReport: NodeStatisticsReport = redistRequests.dequeue()
      initiateLoadRedistribution(nodeReport)
      redistributionInProgress = true
    }
  }

}

