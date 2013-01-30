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

/*
COMMENT: A default built-in implementation of the Planner interface. 
Planner is responsible for coordinating the two-phase dyanmic load balancing
protocol. 
*/
class KeyDistributionPlanner(val appRuntime: AppRuntime) extends LoadPlanner {

  // COMMENT: refers to the size of the history for node statistics report maintained for each node. 
  val maxReports = 5
  val nodeReports = collection.mutable.Map.empty[String, collection.mutable.ListBuffer[NodeStatisticsReport]]
  // COMMENT: A list of node that Planner believes to be overloaded and are candidates for shedding of load. 
  val overloadedHosts = new collection.mutable.ListBuffer[String]
  var redistributionInProgress: Boolean = false

  //COMMENT: Nodes at a periodi interval send statistics report via which they can request for a load transfer. 
  // Planner is the centralized authority that receives all reports and has the discretion in accepting/rejecting 
  // a load transfer request. Currently only a sinlge load transfer activity can be initiated at once.  
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

  /*
   COMMENT: When to accept a load transfer request is critical considering the overhead. 
   In the current implementation, the Planner trusts each node and relies on the set of hot keys reported
   by each node. If a hot key is reported by a node, the corresponding key space is moved to another node. 
   In future, we may opt for a more sophisticated decision tree for accepting/rejecting a load 
   transfer request, or may have alternate implementations of a Planner.  
  */
  def assessNeedToRedistributeLoad(report: NodeStatisticsReport): Array[String] = {
    val redistKeys = report.getRedistKeys()
    val numHosts = RuntimeProvider.appStaticInfo.getSystemHosts().length

    /*
    COMMENT: Only if there are more than 1 host, and hot keys are being reported, 
    that a load redistribution is initiated. 
    */
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
    val rand = new Random()
    val index = rand.nextInt(candidateHosts.size())
    val candidateHost = candidateHosts.get(index)
    val destinationHostIndex = Mupd8Utils.getHostNameToIndex(RuntimeProvider.appStaticInfo, candidateHost)
    destinationHostIndex
   
  }

  /*
   COMMENT: In either case of success of failure, we initiate any pending request, if any
   */
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

