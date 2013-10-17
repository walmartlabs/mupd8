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
import java.net.ServerSocket
import annotation.tailrec

/* Message Server for whole cluster */
class MessageServer(appRun: AppRuntime, port: Int, allSources: Map[String, Source], isTest: Boolean = false) extends Thread with Logging {
  // separate lastCmdID and lastRingUpdateCmdID in case in future
  // there may message not requiring no new ring update
  var lastCmdID = -1
  var lastRingUpdateCmdID = -1
  var ring: HashRing = if (appRun != null) appRun.ring else null
  var keepRunning = true

  setName("MessageServer")
  SendNewRing.start
  AckedNodeCounter.start
  PingCheck.start
  lastCmdID = 0
  if (ring != null)
    SendNewRing ! SendMessageToNode(lastCmdID, ring.ips, (port + 1), NewMessageServerMessage(lastCmdID, appRun.self), port)

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
        debug("Channel - " + channel + " is opened")
        val in = new ObjectInputStream(Channels.newInputStream(channel))
        val out = new ObjectOutputStream(Channels.newOutputStream(channel))
        val msg = in.readObject()
        msg match {
          case NodeChangeMessage(hosts_to_add, hosts_to_remove) =>
            out.writeObject(ACKMessage)
            info("MessageServer: received " + msg)

            // remove completed node changes in request
            val (hosts_to_add1, hosts_to_remove1) = removeChangedHosts(hosts_to_add, hosts_to_remove)
            if (!hosts_to_add1.isEmpty || !hosts_to_remove1.isEmpty) {
              // if there are still some nodes left
              lastCmdID += 1
              lastRingUpdateCmdID = lastCmdID

              // update hash ring
              if (ring == null) {
                ring = HashRing.initFromHosts(hosts_to_add1.toIndexedSeq)
              } else {
                ring = ring.add(hosts_to_add1)
                ring = ring.remove(hosts_to_remove1)
              }
              info("MessageServer: new ring = " + ring)

              // update source
              // check if hostToRemove has source on it
              val ips_to_remove1 = hosts_to_remove1.map(_.ip)
              appRun.startedSources.foreach { source =>
                if (ips_to_remove1.contains(source._2.ip)) {
                  val sourceName = source._1
                  // pick another host from ring to start source on
                  val ipToStart = ring(sourceName)
                  info("NodeRemoveMessage: pick " + ring.ipHostMap(ipToStart) + " to start source" + (sourceName))

                  // if source is not started, send start message
                  // Connect source with hostToStart. If source is not accessible from
                  // this actor, actor will report noderemovemessage to trigger next try
                  appRun.startedSources += (sourceName -> Host(ipToStart, ring.ipHostMap(ipToStart)))
                  val localMSClient = new SendStartSource(sourceName, ipToStart)
                  localMSClient.start
                }
              }

              if (!isTest) {
                info("NodeChangeMessage: CmdID " + lastCmdID + " - Sending " + ring + " to " + ring.ips.map(ring.ipHostMap(_)))
                // start timer and ackcounter
                // reset Timer
                Timer.startTimer(lastCmdID, 2000L, () => {
                  val notAckedNodes = AckedNodeCounter.nodesNotAcked
                  // not all nodes send ack message back
                  info("NodeChangeMessage: " + (lastCmdID, notAckedNodes) + " TIMEOUT")
                  val msClient = new MessageServerClient("localhost", port)
                  msClient.sendMessage(NodeChangeMessage(Set.empty, notAckedNodes.map(ip => Host(ip, ring.ipHostMap(ip)))))
                })
                // reset counter
                AckedNodeCounter ! StartCounter(lastCmdID, appRun, ring.ips, "localhost", port)

                // Send prepare ring update message
                SendNewRing ! SendMessageToNode(lastCmdID, ring.ips, (port + 1), PrepareNodeChangeMessage(lastCmdID, ring.hash, ring.ips, ring.ipHostMap), port)
              }

            } else {
              info("Messageserver: hosts_to_add" + hosts_to_add + " and hosts_to_remove" + hosts_to_remove + " are changed already.")
            }

          case PrepareNodeChangeDoneMessage(cmdID, host) =>
            info("MessageServer: Receive PrepareNodeChangeDoneMessage - " + (cmdID, ring.ipHostMap(host)))
            AckedNodeCounter ! CountPrepareACK(cmdID, host)

          case AllNodesACKedPrepareMessage(cmdID: Int) =>
            info("MessageServer: AllNodesACKedPrepareMessage received, cmdID = " + cmdID)
            // send update ring message to Nodes
            SendNewRing ! SendMessageToNode(cmdID, ring.ips, port + 1, UpdateRing(cmdID), port)

          case IPCHECKDONE =>
            info("MessageServer: IP CHECK received")

          case AskPermitToStartSourceMessage(sourceName, host) =>
            info("MessageServer: Received AskPermitToStartSourceMessage" + (sourceName, host))
            out.writeObject(ACKMessage)
            lastCmdID = lastCmdID + 1
            if (!appRun.startedSources.contains(sourceName)) {
              // if source is not started, send start message
              // Connect source with hostToStart. If source is not accessible from
              // this actor, actor will report noderemovemessage to trigger next try
              appRun.startedSources += (sourceName -> host)
              val localMSClient = new SendStartSource(sourceName, host.ip)
              localMSClient.start
              // send ask new source information to all nodes
              SendNewRing ! SendMessageToNode(lastCmdID, ring.ips, (port + 1), NewSourceMessage(lastCmdID, host, sourceName), port)
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
    SendNewRing ! "EXIT"
    try {
      keepRunning = false
      interrupt
      Thread.sleep(2000)
    } catch { case e: Exception => {} }
  }

  /* actor sending new ring to nodes in cluster */
  abstract class SendNewRingMessage
  case class SendMessageToNode(cmdID: Int, ips: IndexedSeq[String], port: Int, msg: Message, msport: Int) extends SendNewRingMessage
  object SendNewRing extends Actor {
    private val stop = false

    override def act() {
      react {
        // msport: port of message server
        case SendMessageToNode(cmdID, ips, port, msg, msport) =>
          var nodesFailedToSend: Set[Host] = Set.empty
          ips foreach (ip =>
            // check it is still latest command and hash ring
            if (cmdID == lastRingUpdateCmdID) {
              info("SendNewRing: Sending cmdID " + cmdID + ", msg = " + msg + " to endpoint " + (ring.ipHostMap(ip), port))
              val lmsclient = new LocalMessageServerClient(ip, port)
              if (!lmsclient.sendMessage(msg)) {
                // collect host fails
                info("SendNewRing: report " + ring.ipHostMap(ip) + " fails")
                nodesFailedToSend = nodesFailedToSend + Host(ip, ring.ipHostMap(ip))
              }
            } else info("SendNewRing: skip non-currernt command - " + lastRingUpdateCmdID + ", " + ring))
          if (!nodesFailedToSend.isEmpty){
            info("SendNewRing: failed to send new ring to couple node, send NodeChangeMessage to messageserver")
            val msClient = new MessageServerClient("localhost", msport)
            if (!msClient.sendMessage(NodeChangeMessage(Set.empty, nodesFailedToSend))) {

            }
          }
          act()
        case "EXIT" =>
          info("SendNewRing receive EXIT")
        case msg =>
          error("MSG - " + msg + " is not supported now")
      }
    }
  }

  class SendStartSource(sourceName: String, ipToStart: String) extends Actor {
    override def act() {
      val client = new LocalMessageServerClient(ipToStart, port + 1)
      info("SendStartSource: sending StartSourceMessage " + (sourceName) + " to " + (ipToStart, port + 1))
      if (!client.sendMessage(StartSourceMessage(sourceName))) {
        // report host fails if any exception happens
        error("SendStartSource: report " + ipToStart + " fails")
        val msClient = new MessageServerClient("localhost", port)
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
        appRun.startedSources.foreach{source =>
          // send ping message
          val lmsClient = new LocalMessageServerClient(source._2.ip, port + 1)
          if (!lmsClient.sendMessage(PING())) {
            // report host fails if any exception happens
            error("SendStartSource: report " + source._2 + " fails")
            failedNodes = failedNodes + source._2
          }
        }
        if (!failedNodes.isEmpty) {
          val msClient = new MessageServerClient("localhost", port)
          msClient.sendMessage(NodeChangeMessage(Set.empty, failedNodes))
        }
        Thread.sleep(2000)
      }
    }
  }
}

/* Message Server for every node, which receives ring update message for now */
class LocalMessageServer(port: Int, runtime: AppRuntime) extends Runnable with Logging {
  private var lastCmdID = -1
  private var lastCommittedCmdID = -1

  override def run() {
    def setCandidateRingAndHostList(hash: IndexedSeq[String], hosts: (IndexedSeq[String], Map[String, String])) {
      runtime.candidateRing = HashRing.initFromParameters(hosts._1, hash, hosts._2)
    }

    info("LocalMessageServerThread: Start listening to " + port)
    val serverSocketChannel = ServerSocketChannel.open()
    serverSocketChannel.socket().bind(new InetSocketAddress(port))
    debug("LocalMessageServer started, listening to " + port)
    while (true) {
      try {
        val channel = serverSocketChannel.accept()
        val in = new ObjectInputStream(Channels.newInputStream(channel))
        val out = new ObjectOutputStream(Channels.newOutputStream(channel))
        val msg = in.readObject
        msg match {
          case PrepareNodeChangeMessage(cmdID, hashInNewRing, iPsInNewRing, iP2HostMap) =>
            info("LocalMessageServer: Received " + msg)
            if (cmdID > lastCmdID) {
              // set candidate ring
              setCandidateRingAndHostList(hashInNewRing, (iPsInNewRing, iP2HostMap))
              info("LocalMessageserver - candidateRing = " + runtime.candidateRing)

              // wait until current performer job is done
              debug("Checking current performer job")
              runtime.waitPerformerJobsDone

              // flush dirty slates
              debug("PrepareNodeChangeMessage - going to flush cassandra")
              runtime.flushFilteredDirtySlateToCassandra
              lastCmdID = cmdID

              // send ACKPrepareNodeChangeMessage back to message server7
              debug("LocalMessageServer: CMD " + cmdID + " - Update Ring with " + iP2HostMap)
              runtime.msClient.sendMessage(PrepareNodeChangeDoneMessage(cmdID, runtime.self.ip))
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
              if (runtime.candidateRing != null) {
                // send commit spam to all other nodes
                new SendCommitSpam(runtime.candidateRing.ips, cmdID).start

                val newIPs: Set[String] = runtime.candidateRing.ips.toSet
                val oldIPs: Set[String] = if (runtime.appStatic.systemHosts == null) Set.empty else runtime.appStatic.systemHosts.keySet
                val addedIPs = setAminusSetB(newIPs, oldIPs, Set.empty)
                val removedIPs = setAminusSetB(oldIPs, newIPs, Set.empty)
                debug("UpdateRing: addedIPs = " + addedIPs + ", removedIPs = " + removedIPs)

                // Adjust nodes in cluster before switch ring
                // o.w. events for addedIPs might be able to be sent new nodes
                if (runtime.pool != null) {
                  runtime.pool.cluster.addHosts(addedIPs)
                  runtime.pool.cluster.removeHosts(removedIPs)
                }

                runtime.ring = runtime.candidateRing
                runtime.appStatic.systemHosts = runtime.candidateRing.ipHostMap
                runtime.candidateRing = null
                runtime.flushSlatesInBufferToQueue
                lastCommittedCmdID = cmdID
                info("LocalMessageServer: cmdID - " + cmdID + " update ring done, new ring = " + runtime.ring)
              } else {
                warn("UpdateRing: candidate ring is null in UpdateRing")
              }
            }

          case StartSourceMessage(sourceName) =>
            info("LocalMessageServer: Received " + msg)
            out.writeObject(ACKMessage)
            runtime.startSource(sourceName)

          case ToBeNextMessageSeverMessage(requestedBy) =>
            info("LocalMessageServer: Received " + msg)
            out.writeObject(ACKMessage)
            runtime.startMessageServer()

          case NewMessageServerMessage(cmdID, messageServer) =>
            info("LocalMessageServer: Received " + msg)
            out.writeObject(ACKMessage)
            runtime.messageServerHost = messageServer.ip
            lastCmdID = cmdID
            lastCommittedCmdID = -1

          case NewSourceMessage(cmdID, host, sourceName) =>
            info("LocalMessageServer: Recieved " + msg)
            runtime.startedSources = runtime.startedSources + (sourceName -> host)

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
