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

package com.walmartlabs.mupd8

import java.nio.channels.Channels
import java.nio.channels.ServerSocketChannel
import java.net.InetSocketAddress
import java.io._
import java.util.concurrent.Executors
import java.util.concurrent.ExecutorService
import joptsimple._
import java.io.FileInputStream
import java.io.FileOutputStream
import java.io.IOException
import java.io.ObjectInputStream
import java.io.ObjectOutputStream
import grizzled.slf4j.Logging
import scala.actors.Actor
import scala.actors.Actor._
import scala.collection.JavaConverters._
import scala.util.Failure
import scala.util.Success
import scala.util.Try
import java.net.ServerSocket
import annotation.tailrec
import java.util.TimerTask

/* Message Server for whole cluster */
class MessageServer(appRuntime: AppRuntime, port: Int, allSources: Map[String, Source], isTest: Boolean = false) extends Thread with Logging {
  // separate lastCmdID and lastRingUpdateCmdID in case in future
  // there may message not requiring no new ring update
  var lastCmdID = -1
  var lastRingUpdateCmdID = -1
  var ring: HashRing = if (appRuntime != null) appRuntime.ring else null
  var lastMessageServer: Host = null
  var keepRunning = true
  var sender: SendMessage = null
  var ackCounter: AckCounter = null

  setName("MessageServer")

  if (appRuntime != null) {
    lastMessageServer = appRuntime.messageServerHost
    appRuntime.messageServerHost = appRuntime.self
  }

  PingCheck.start
  lastCmdID = 0
  if (ring != null) {
    sender = new SendMessage(appRuntime, ring, lastCmdID, ring.ips.filter(ip => lastMessageServer == null || ip.compareTo(lastMessageServer.ip) != 0), (port + 1), NewMessageServerMessage(lastCmdID, appRuntime.self), 90 * 1000)
    sender.send()
  }

  /* socket server to communicate clients */
  override def run() {
    info("MessageServerThread: Start listening to :" + port)
    val serverSocketChannel = ServerSocketChannel.open()
    serverSocketChannel.socket().bind(new InetSocketAddress(port))
    debug("server started, listening " + port)

    // Incoming messages need to be processed sequentially
    // since it might cause hash ring accordingly. So NOT
    // generate one thread for each incoming request.
    while (keepRunning) {
      try {
        val channel = serverSocketChannel.accept()
        val remote = channel.socket().getRemoteSocketAddress()
        debug("Channel - " + channel + " is opened")
        val in = new ObjectInputStream(Channels.newInputStream(channel))
        val out = new ObjectOutputStream(Channels.newOutputStream(channel))
        val msg = in.readObject()
        msg match {
          case NodeChangeMessage(hosts_to_add, hosts_to_remove) =>
            out.writeObject(ACKMessage)
            info("MessageServer: received " + msg + " from " + remote)

            // remove completed node changes in request
            val (hosts_to_add1, hosts_to_remove1) = removeChangedHosts(hosts_to_add, hosts_to_remove)
            info("MessageServer: hosts to add = " + hosts_to_add1 + ", hosts to remove = " + hosts_to_remove1)
            if (!hosts_to_add1.isEmpty || !hosts_to_remove1.isEmpty) {
              // if there are still some nodes left
              lastCmdID += 1
              lastRingUpdateCmdID = lastCmdID

              // update hash ring
              if (ring == null) {
                ring = HashRing.initFromHosts(hosts_to_add1.toIndexedSeq)
              } else {
                ring = ring.add(hosts_to_add1)
                info("MessageServer: 1 new ring = " + ring + ", ip2host = " + ring.ipHostMap)
                ring = ring.remove(hosts_to_remove1)
                info("MessageServer: 2 new ring = " + ring + ", ip2host = " + ring.ipHostMap)
              }

              if (!isTest) {
                info("NodeChangeMessage: CmdID " + lastCmdID + " - Sending " + ring + " to " + ring.ips.map(ring.ipHostMap(_)))
                if (ackCounter != null) ackCounter.stop()
                ackCounter = new AckCounter(appRuntime, ring, lastCmdID, ring.ips)
                ackCounter.startCount()

                // Send prepare ring update message
                if (sender != null) sender.stop()
                sender = new SendMessage(appRuntime, ring, lastCmdID, ring.ips, (port + 1), PrepareNodeChangeMessage(lastCmdID, ring.hash, ring.ips, ring.ipHostMap), 90 * 1000)
                sender.send()
              }
            } else {
              info("Messageserver: hosts_to_add" + hosts_to_add + " and hosts_to_remove" + hosts_to_remove + " are changed already.")
            }

            // update source
            // check if there is any node in delete list which had source reader on it before
            val ips_to_remove = hosts_to_remove.map(_.ip)
            appRuntime.startedSources.foreach { source =>
              if (ips_to_remove.contains(source._2.ip)) {
                val sourceName = source._1
                // pick another host from ring to start source on
                val ipToStart = ring(sourceName)
                info("NodeChangeMessage: pick " + ring.ipHostMap(ipToStart) + " to start source" + (sourceName))

                // if source is not started, send start message
                // Connect source with hostToStart. If source is not accessible from
                // this actor, actor will report noderemovemessage to trigger next try
                appRuntime.startedSources += (sourceName -> Host(ipToStart, ring.ipHostMap(ipToStart)))

                // convert started sources map to string
                // by adding 0x1d between key and value and \n between each pair
                val sources2 = appRuntime.startedSources map(p => p._1 -> p._2.ip)
                val sources3 = sources2.foldLeft(""){ (str, p) => str + p._1 + 0x1d.toChar + p._2 + "\n" }
                // write started sources into db store
                appRuntime.storeIO.writeColumn(appRuntime.appStatic.cassColumnFamily, CassandraPool.PRIMARY_ROWKEY, CassandraPool.STARTED_SOURCES, sources3)

                val localMSClient = new SendStartSource(sourceName, ipToStart)
                localMSClient.start
              }
            }

          case PrepareNodeChangeDoneMessage(cmdID, hostip) =>
            info("MessageServer: Receive PrepareNodeChangeDoneMessage - " + (cmdID, hostip) + " from " + remote)
            if (cmdID < lastCmdID) {
              info("PrepareNodeChangeDoneMessage is too old, igore it. - " + cmdID)
            } else {
              ackCounter.count(cmdID, hostip)
            }

          case AllNodesACKedPrepareMessage(cmdID: Int) =>
            info("MessageServer: AllNodesACKedPrepareMessage received, cmdID = " + cmdID)
            if (cmdID < lastCmdID) {
              info("AllNodesAckedPrepareMessage %d received, but lastcmdID is %d".format(cmdID, lastCmdID))
            } else {
              // send update ring message to Nodes
              if (sender != null) sender.stop()
              sender = new SendMessage(appRuntime, ring, cmdID, ring.ips, port + 1, UpdateRing(cmdID), 90 * 1000)
              sender.send()
            }

          case IPCHECKDONE =>
            info("MessageServer: IP CHECK received")

          case AskPermitToStartSourceMessage(sourceName, host) =>
            info("MessageServer: Received AskPermitToStartSourceMessage" + (sourceName, host) + " from " + remote)
            out.writeObject(ACKMessage)
            lastCmdID = lastCmdID + 1
            if (!appRuntime.startedSources.contains(sourceName)) {
              // if source is not started, send start message
              // Connect source with hostToStart. If source is not accessible from
              // this actor, actor will report noderemovemessage to trigger next try
              appRuntime.startedSources += (sourceName -> host)
              new SendStartSource(sourceName, host.ip).start
              // convert started sources map to string
              // by adding 0x1d between key and value and \n between each pair
              val sources2 = appRuntime.startedSources map(p => p._1 -> p._2.ip)
              val sources3 = sources2.foldLeft(""){ (str, p) => str + p._1 + 0x1d.toChar + p._2 + "\n" }
              // write started sources into db store
              appRuntime.storeIO.writeColumn(appRuntime.appStatic.cassColumnFamily, CassandraPool.PRIMARY_ROWKEY, CassandraPool.STARTED_SOURCES, sources3)
            }

          case _ => error("MessageServerThread: Not a valid msg: " + msg)
        }
        out.close; in.close; channel.close
      } catch {
        case e: java.nio.channels.ClosedByInterruptException => info("MessageServerThread is interrupted by io")
        case e: Exception => error("MessageServerThread exception.", e)
      }
    }

    serverSocketChannel.close
  }

  // remove hosts already added and removed in latest ring
  def removeChangedHosts(hosts_to_add: Set[Host], hosts_to_remove: Set[Host]): (Set[Host], Set[Host]) = {
    if (ring != null) {
      if (!(hosts_to_add & hosts_to_remove).isEmpty) {
        error("MessageServer: hosts_to_add," + hosts_to_add + " , an hosts_to_remove," + hosts_to_remove + " , conflict")
        (Set.empty, Set.empty)
      } else {
        (hosts_to_add.filter(host => !ring.ips.contains(host.ip)),
         hosts_to_remove.filter(host => ring.ips.contains(host.ip)))
      }
    } else {
      (hosts_to_add, Set.empty)
    }
  }

  def shutdown() = {
    info("Initiate shutdown")
    try {
      keepRunning = false
      interrupt
      Thread.sleep(2000)
    } catch { case e: Exception => {} }
  }

  /* actor sending new ring to nodes in cluster */
  /* send message async and timeout */

  class SendStartSource(sourceName: String, ipToStart: String) extends Actor {
    override def act() {
      val client = new LocalMessageServerClient(ipToStart, port + 1)
      info("SendStartSource: sending StartSourceMessage " + (sourceName) + " to " + (ipToStart, port + 1))
      if (!client.sendMessage(StartSourceMessage(sourceName))) {
        // report host fails if any exception happens
        error("SendStartSource: report " + ipToStart + " fails")
        val msClient = new MessageServerClient(appRuntime)
        msClient.sendMessage(NodeChangeMessage(Set.empty, Set(Host(ipToStart, ring.ipHostMap(ipToStart)))))
      }
    }
  }

  // For now it only check node with sources on it
  // From there those nodes can detect other nodes' failure
  object PingCheck extends Actor {
    override def act() {
      while (keepRunning) {
        var failedNodes: Set[Host] = Set.empty
        appRuntime.startedSources.foreach{source =>
          // send ping message
          val lmsClient = new LocalMessageServerClient(source._2.ip, port + 1)
          if (!lmsClient.sendMessage(PING())) {
            // report host fails if any exception happens
            error("PingCheck: report " + source._2 + " fails")
            failedNodes = failedNodes + source._2
          }
        }
        if (!failedNodes.isEmpty) {
          val msClient = new MessageServerClient(appRuntime)
          msClient.sendMessage(NodeChangeMessage(Set.empty, failedNodes))
        }
        Thread.sleep(2000)
      }
    }
  }
}

/* Message Server for every node, which receives ring update message for now */
class LocalMessageServer(port: Int, appRuntime: AppRuntime) extends Runnable with Logging {
  private var lastCmdID = -1
  private var lastCommittedCmdID = -1

  override def run() {
    def setCandidateRingAndHostList(hash: IndexedSeq[String], hosts: (IndexedSeq[String], Map[String, String])) {
      appRuntime.candidateRing = HashRing.initFromParameters(hosts._1, hash, hosts._2)
    }

    info("LocalMessageServerThread: Start listening to " + port)
    val serverSocketChannel = ServerSocketChannel.open()
    serverSocketChannel.socket().bind(new InetSocketAddress(port))
    debug("LocalMessageServer started, listening to " + port)
    while (true) {
      try {
        val channel = serverSocketChannel.accept()
        val remote = channel.socket().getRemoteSocketAddress()
        val in = new ObjectInputStream(Channels.newInputStream(channel))
        val out = new ObjectOutputStream(Channels.newOutputStream(channel))
        val msg = in.readObject
        msg match {
          case PrepareNodeChangeMessage(cmdID, hashInNewRing, iPsInNewRing, iP2HostMap) =>
            info("LocalMessageServer: Received " + msg + " from " + remote)
            if (cmdID > lastCmdID) {
              // set candidate ring
              setCandidateRingAndHostList(hashInNewRing, (iPsInNewRing, iP2HostMap))
              info("LocalMessageserver - candidateRing = " + appRuntime.candidateRing)

              // wait until current performer job is done
              debug("Checking current performer job")
              appRuntime.waitPerformerJobsDone

              // flush dirty slates
              debug("PrepareNodeChangeMessage - going to flush cassandra")
              appRuntime.flushFilteredDirtySlateToCassandra
              lastCmdID = cmdID

              // send ACKPrepareNodeChangeMessage back to message server7
              debug("PrepareNodeChangeMessage - Update Candidate Ring with " + iP2HostMap)
              appRuntime.msClient.sendMessage(PrepareNodeChangeDoneMessage(cmdID, appRuntime.self.ip))
              info("LocalMessageServer: PrepareNodeChangeMessage - CMD " + cmdID + " - Sent ACKPrepareNodeChangeMessage to message server")
            } else
              error("LocalMessageServer: current cmd, " + cmdID + " is younger than lastCmdID, " + lastCmdID)

          case UpdateRing(cmdID) =>
            @tailrec
            def setAminusSetB(setA: Set[String], setB: Set[String], rlt: Set[String]): Set[String] = {
              if (setA.isEmpty) rlt
              else {
                if (setB.contains(setA.head))
                  setAminusSetB(setA.tail, setB, rlt)
                else
                  setAminusSetB(setA.tail, setB, rlt + setA.head)
              }
            }

            info("LocalMessageServer: Received " + msg)
            out.writeObject(ACKMessage)
            if (lastCommittedCmdID >= cmdID) {
              info("LocalMessageserver: cmd " + cmdID + " has already been committed")
            } else {
              if (appRuntime.candidateRing != null) {
                // send commit spam to all other nodes
                new SendCommitSpam(appRuntime.candidateRing.ips, cmdID).start

                val newIPs: Set[String] = appRuntime.candidateRing.ips.toSet
                val oldIPs: Set[String] = if (appRuntime.appStatic.systemHosts == null) Set.empty else appRuntime.appStatic.systemHosts.keySet
                val addedIPs = setAminusSetB(newIPs, oldIPs, Set.empty)
                val removedIPs = setAminusSetB(oldIPs, newIPs, Set.empty)
                debug("UpdateRing: addedIPs = " + addedIPs + ", removedIPs = " + removedIPs)

                // Adjust nodes in cluster before switch ring
                // o.w. events for addedIPs might be able to be sent new nodes
                if (appRuntime.pool != null) {
                  appRuntime.pool.cluster.addHosts(addedIPs)
                  appRuntime.pool.cluster.removeHosts(removedIPs)
                }

                appRuntime.ring = appRuntime.candidateRing
                appRuntime.appStatic.systemHosts = appRuntime.candidateRing.ipHostMap
                appRuntime.candidateRing = null
                appRuntime.flushSlatesInBufferToQueue
                lastCommittedCmdID = cmdID
                info("LocalMessageServer: cmdID - " + cmdID + " update ring done, new ring = " + appRuntime.ring)
              } else {
                warn("UpdateRing: candidate ring is null in UpdateRing")
              }
            }

          case StartSourceMessage(sourceName) =>
            info("LocalMessageServer: Received " + msg)
            out.writeObject(ACKMessage)
            appRuntime.startSource(sourceName)

          case ToBeNextMessageSeverMessage(requestedBy) =>
            info("LocalMessageServer: Received " + msg)
            out.writeObject(ACKMessage)
            if (appRuntime.startedMessageServer) {
              info("Message server has been started on this node")
            } else {
              val prevMessageServer = appRuntime.messageServerHost.ip
              debug("LocalMessageServer: prevMessageServer = " + prevMessageServer + ", ipHostMap = " + appRuntime.ring.ipHostMap)
              appRuntime.startMessageServer()
              // write itself as message server into db store
              appRuntime.storeIO.writeColumn(appRuntime.appStatic.cassColumnFamily, CassandraPool.PRIMARY_ROWKEY, CassandraPool.MESSAGE_SERVER, appRuntime.self.ip)
              Thread.sleep(5000) // yield cpu to message server thread
              // load started sources from db store
              Try(appRuntime.storeIO.fetchStringValueColumn(appRuntime.appStatic.cassColumnFamily, CassandraPool.PRIMARY_ROWKEY, CassandraPool.STARTED_SOURCES)) match {
                case Failure(ex) =>
                  error("ToBeNextMessageServerMessage: fetch started sources failed", ex); appRuntime.startedSources = Map.empty
                case Success(str) =>
                  // load started sources from db store
                  // one pair format: key + 0x1d + value + \n
                  val m = str.lines.map { str => val arr = str.split(0x1d.toChar); arr(0) -> arr(1) }.toMap
                  val m1 = m.filter(p => appRuntime.ring.ips.contains(p._2))
                  appRuntime.startedSources = m1.map(p => p._1 -> Host(p._2, appRuntime.ring.ipHostMap(p._2)))
                  info("LocalMessageServer: started sources from db store = " + appRuntime.startedSources)
              }
              info("LocalMessageServer: write messageserver " + appRuntime.self.hostname + " to data store")
            }

          case NewMessageServerMessage(cmdID, messageServer) =>
            info("LocalMessageServer: Received " + msg)
            out.writeObject(ACKMessage)
            appRuntime.messageServerHost = messageServer
            appRuntime.msClient = new MessageServerClient(appRuntime, 1000)
            lastCmdID = cmdID
            lastCommittedCmdID = -1
            debug("LocalMessageServer: set messageServerHost to " + messageServer)

          case PING() =>
            trace("Received PING")

          case _ => error("LocalMessageServer: Not a valid msg, " + msg.toString)
        }
        in.close
        out.close
        channel.close
      } catch {
        case e : Exception => error("LocalMessageServer exception", e)
      }
    }
  }

  class SendCommitSpam(ips: Seq[String], cmdID: Int) extends Actor {
    override def act() {
      ips.foreach { ip =>
        val lmsClient = new LocalMessageServerClient(ip, port)
        lmsClient.sendMessage(UpdateRing(cmdID))
      }
    }
  }

}
