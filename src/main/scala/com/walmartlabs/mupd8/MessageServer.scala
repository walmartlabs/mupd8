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

//import java.nio.channels._
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
import scala.collection._
import scala.collection.JavaConverters._
import java.net.ServerSocket

/* Message Server for whole cluster */
class MessageServer(port: Int, allSources: immutable.Map[String, Source], isTest: Boolean = false) extends Thread with Logging {

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
          case NodeRemoveMessage(ipToRemove) =>
            info("MessageServer: received node remove message: " + msg)
            // check if node is already removed
            if (!ring2.iPs.contains(ipToRemove)) {
              info("MessageServer: NodeRemoveMessage - " + ipToRemove + " is already removed in message server")
              out.writeObject(ACKNodeRemove(ipToRemove))
            } else {
              lastCmdID += 1
              lastRingUpdateCmdID = lastCmdID
              // send ACK to reported
              out.writeObject(ACKNodeRemove(ipToRemove))
              // update hash ring
              val newHostList: IndexedSeq[String] = ring2.iPs filter (host => host.compareTo(ipToRemove) != 0)
              ring2 = ring2.remove(newHostList, ipToRemove)

              // update source
              // check if hostToRemove has source on it
              startedSources.foreach { source =>
                if (source._2.compareTo(ipToRemove) == 0) {
                  val sourceName = source._1
                  // pick another host from ring to start source on
                  val ipToStart = ring2(sourceName)
                  info("NodeRemoveMessage: pick " + ring2.ipHostMap(ipToStart) + " to start source" + (sourceName))

                  // if source is not started, send start message
                  // Connect source with hostToStart. If source is not accessible from
                  // this actor, actor will report noderemovemessage to trigger next try
                  startedSources += (sourceName -> ipToStart)
                  val localMSClient = new SendStartSource(sourceName, ipToStart)
                  localMSClient.start
                }
              }

              if (!isTest) {
                // if it is unit test, don't send new ring to all nodes
                // Local message server's port is always port + 1
                info("NodeRemoveMessage: CmdID " + lastCmdID + " - Sending " + ring2 + " to " + ring2.iPs.map(ring2.ipHostMap(_)))

                // reset Timer
                TimerActor.stopTimer(lastCmdID - 1, "cmdID: " + lastCmdID)
                TimerActor.startTimer(lastCmdID, 2000L, () => info("TIMEOUT"))
                // reset counter
                AckedNodeCounter ! StartCounter(lastCmdID, ring2.iPs, "localhost", port)

                // Send prepare ring update message
                SendNewRing ! SendMessageToNode(lastCmdID, ring2.iPs, (port + 1), PrepareRemoveHostMessage(lastCmdID, ipToRemove, ring2.hash, ring2.iPs, ring2.ipHostMap), port)
              }
            }

          case NodeJoinMessage(hostToAdd) =>
            info("MessageServer: Received node join message: " + msg)
            lastCmdID += 1
            lastRingUpdateCmdID = lastCmdID
            // send ACK to reported
            out.writeObject(ACKNodeJoin(hostToAdd.ip))
            // update hash ring
            ring2 = if (ring2 == null) HashRing2.initFromHost(hostToAdd)
            else if (ring2.iPs.contains(hostToAdd.ip)) {
              ring2 // if ring2 already contains this node, do nothing
            } else {
              val newHostList = ring2.iPs :+ hostToAdd.ip
              ring2.add(newHostList, hostToAdd)
            }
            // TODO: replace isTest with new SendNewRing
            if (!isTest) {
              // if it is unit test, don't send new ring to all nodes
              // Local message server's port is always port + 1
              info("NodeJoinMessage: cmdID " + lastCmdID + " - Sending " + ring2 + " to " + ring2.iPs.map(ring2.ipHostMap(_)))
              // reset Timer
              TimerActor.stopTimer(lastCmdID - 1, "cmdID: " + lastCmdID)
              TimerActor.startTimer(lastCmdID, 5000L, () => info("TIMEOUT")) // TODO: replace info
              // reset counter
              AckedNodeCounter ! StartCounter(lastCmdID, ring2.iPs, "localhost", port)

              // Send prepare ring update message
              SendNewRing ! SendMessageToNode(lastCmdID, ring2.iPs, port + 1, PrepareAddHostMessage(lastCmdID, hostToAdd, ring2.hash, ring2.iPs, ring2.ipHostMap), port)
            }

          case ACKPrepareAddHostMessage(cmdID, host) =>
            info("MessageServer: Receive ACKPrepareAddHostMessage - " + (cmdID, ring2.ipHostMap(host)))
            AckedNodeCounter ! CountPrepareACK(cmdID, host)

          case ACKPrepareRemoveHostMessage(cmdID, host) =>
            info("MessageServer: Receive ACKPrepareRemoveHostMessage - " + (cmdID, ring2.ipHostMap(host)))
            AckedNodeCounter ! CountPrepareACK(cmdID, host)

          case AllNodesACKedPrepareMessage(cmdID: Int) =>
            info("MessageServer: AllNodesACKedPrepareMessage received, cmdID = " + cmdID)
            // send update ring message to Nodes
            SendNewRing ! SendMessageToNode(cmdID, ring2.iPs, port + 1, UpdateRing(cmdID), port)

          case IPCHECKDONE =>
            info("MessageServer: IP CHECK received")

          case AskPermitToStartSourceMessage(sourceName, host) =>
            info("MessageServer: Received AskPermitToStartSourceMessage" + (sourceName, host))
            out.writeObject(ACKMessage)
            if (!startedSources.contains(sourceName)) {
              // if source is not started, send start message
              // Connect source with hostToStart. If source is not accessible from
              // this actor, actor will report noderemovemessage to trigger next try
              startedSources += (sourceName -> host.ip)
              val localMSClient = new SendStartSource(sourceName, host.ip)
              localMSClient.start
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
  case class SendMessageToNode(cmdID: Int, iPs: IndexedSeq[String], port: Int, msg: Message, msport: Int) extends SendNewRingMessage
  object SendNewRing extends Actor {
    private val stop = false

    override def act() {
      react {
        // msport: port of message server
        case SendMessageToNode(cmdID, iPs, port, msg, msport) =>
          iPs foreach (ip =>
            // check it is still latest command and hash ring
            if (cmdID == lastRingUpdateCmdID) {
              info("SendNewRing: Sending cmdID " + cmdID + ", msg = " + msg + " to endpoint " + (ring2.ipHostMap(ip), port))
              val client = new LocalMessageServerClient(ip, port)
              if (!client.sendMessage(msg)) {
                // report host fails if any exception happens
                info("SendNewRing: report " + ring2.ipHostMap(ip) + " fails")
                val msClient = new MessageServerClient("localhost", msport)
                msClient.sendMessage(NodeRemoveMessage(ip))
              }
            } else info("SendNewRing: skip non-currernt command - " + lastRingUpdateCmdID + ", " + ring2))
          act()
        case "EXIT" =>
          info("SendNewRing receive EXIT")
        case msg =>
          error("MSG - " + msg + " is not supported now")
      }
    }
  }
  SendNewRing.start

  class SendStartSource(sourceName: String, ipToStart: String) extends Actor {
    override def act() {
      val client = new LocalMessageServerClient(ipToStart, port + 1)
      info("SendStartSource: sending StartSourceMessage " + (sourceName) + " to " + (ipToStart, port + 1))
      if (!client.sendMessage(StartSourceMessage(sourceName))) {
        // report host fails if any exception happens
        error("SendStartSource: report " + ipToStart + " fails")
        val msClient = new MessageServerClient("localhost", port)
        msClient.sendMessage(NodeRemoveMessage(ipToStart))
      }
    }
  }

  // separate lastCmdID and lastRingUpdateCmdID in case in future
  // there may message not requiring no new ring update
  var lastCmdID = -1
  var lastRingUpdateCmdID = -1
  var ring2: HashRing2 = null
  var keepRunning = true
  // save started source readers
  var startedSources: Map[String, String] = Map.empty // (source name -> source host machine ip)

  AckedNodeCounter.start

  object PingCheck extends Actor {
    override def act() {
      while (keepRunning) {
        startedSources.foreach{source =>
          // send ping message
          val lmsClient = new LocalMessageServerClient(source._2, port + 1)
          if (!lmsClient.sendMessage(PING())) {
            // report host fails if any exception happens
            error("SendStartSource: report " + source._2 + " fails")
            val msClient = new MessageServerClient("localhost", port)
            msClient.sendMessage(NodeRemoveMessage(source._2))
          }
        }
        Thread.sleep(2000)
      }
    }
  }
  PingCheck.start
}

object MessageServer extends Logging {

  def main(args: Array[String]) {
    val parser = new OptionParser
    val folderOpt = parser.accepts("d", "REQUIRED: config file folder containing sys and app configs").withRequiredArg().ofType(classOf[String])
    val sysOpt = parser.accepts("s", "DEPRECATED: sys config file").withRequiredArg().ofType(classOf[String])
    val appOpt = parser.accepts("a", "DEPRECATED: app config file").withRequiredArg().ofType(classOf[String])
    val pidOpt = parser.accepts("pidFile", "mupd8 process PID file").withRequiredArg().ofType(classOf[String]).defaultsTo("messageserver.pid")
    val options = parser.parse(args: _*)

    var config: application.Config = null
    if (options.has(folderOpt)) {
      info(folderOpt + " is provided")
      config = new application.Config(new File(options.valueOf(folderOpt)))
    } else if (options.has(sysOpt) && options.has(appOpt)) {
      info(sysOpt + " and " + appOpt + " are provided")
      config = new application.Config(options.valueOf(sysOpt), options.valueOf(appOpt))
    } else {
      error("Missing arguments: Please provide either " +
        folderOpt + " (" + folderOpt.description + ") or " +
        sysOpt + " (" + sysOpt.description + ") and " +
        appOpt + " (" + appOpt.description + ")")
      System.exit(1)
    }
    val host = Option(config.getScopedValue(Array("mupd8", "messageserver", "host"))).getOrElse("").asInstanceOf[String]
    val port = Option(config.getScopedValue(Array("mupd8", "messageserver", "port"))).getOrElse(-1).asInstanceOf[Long].toInt
    val sources = Option(config.getScopedValue(Array("mupd8", "sources"))).map {
      x => x.asInstanceOf[java.util.List[org.json.simple.JSONObject]]
    }.getOrElse(new java.util.ArrayList[org.json.simple.JSONObject]()).asScala
    if (host.isEmpty || port < 0) {
      error("MessageServer: host or port is None - " + (host, port))
      System.exit(-1)
    }

    if (options.has(pidOpt)) Misc.writePID(options.valueOf(pidOpt))

    if (Misc.isLocalHost(host)) {
      // save all listed sources
      val allSources = (for {
        source <- sources;
        sourceName = source.get("name").asInstanceOf[String];
        sourcePerformer = source.get("performer").asInstanceOf[String];
        sourceClass = source.get("source").asInstanceOf[String];
        params = source.get("parameters").asInstanceOf[java.util.List[String]].asScala.toList
      } yield (sourceName -> new Source(sourceName, sourceClass, sourcePerformer, params))).toMap

      // start server thread
      val server = new MessageServer(Integer.valueOf(port), allSources)
      server.start

      Runtime.getRuntime().addShutdownHook(new Thread { override def run = server.shutdown() })
    } else {
      info("It is not a message server host")
      System.exit(0);
    }
  }
}

/* Message Server for every node, which receives ring update message for now */
class LocalMessageServer(port: Int, runtime: AppRuntime) extends Runnable with Logging {
  private var lastCmdID = -1;

  override def run() {
    def setCandidateRingAndHostList(hash: IndexedSeq[String], hosts: (IndexedSeq[String], immutable.Map[String, String])) {
      runtime.candidateRing = new HashRing(hash)
      runtime.candidateHostList = hosts
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
          // PrepareAddHostMessage: accept new ring from message server and prepare for switching
          case PrepareAddHostMessage(cmdID, addedHost, hashInNewRing, iPsInNewRing, iP2HostMap) =>
            info("LocalMessageServer: Received " + msg)
            if (cmdID > lastCmdID) {
              // set candidate ring
              setCandidateRingAndHostList(hashInNewRing, (iPsInNewRing, iP2HostMap))
              // wait until current performer job is done
              debug("Checking current performer job")
              runtime.waitPerformerJobsDone
              // flush dirty slates
              debug("PrepareAddHostMessage - going to flush cassandra")
              runtime.flushFilteredDirtySlateToCassandra
              if (runtime.pool != null) {
                // update mucluster if node is up already
                info("LocalMessageServer: cmdID " + cmdID + ", Addhost " + addedHost + " to mucluster")
                runtime.pool.cluster.addHost(addedHost.ip)
              }
              lastCmdID = cmdID
              debug("LocalMessageServer: CMD " + cmdID + " - Update Ring with " + iP2HostMap)
              runtime.msClient.sendMessage(ACKPrepareAddHostMessage(cmdID, runtime.appStatic.self.ip))
              info("LocalMessageServer: PrepareAddHostMessage - CMD " + cmdID + " - Sent ACKPrepareAddHostMessage to message server")
            }  else
              error("LocalMessageServer: current cmd, " + cmdID + " is younger than lastCmdID, " + lastCmdID)

          case PrepareRemoveHostMessage(cmdID, removedIP, hashInNewRing, iPsInNewRing, iP2HostMap) =>
            info("LocalMessageServer: Received " + msg)
            if (cmdID > lastCmdID) {
              // set candidate ring
              setCandidateRingAndHostList(hashInNewRing, (iPsInNewRing, iP2HostMap))
              // wait until current performer job is done
              debug("Checking current performer job")
              runtime.waitPerformerJobsDone
              // flush dirty slates
              debug("PrepareRemoveHostMessage - going to flush cassandra")
              runtime.flushFilteredDirtySlateToCassandra
              if (runtime.pool != null) {
                // update mucluster if node is up already
                info("LocalMessageServer: PrepareRemoveHostMessage - cmdID " + cmdID + ", remove host " + removedIP + " from mucluster")
                runtime.pool.cluster.removeHost(removedIP)
              }
              lastCmdID = cmdID
              runtime.msClient.sendMessage(ACKPrepareRemoveHostMessage(cmdID, runtime.appStatic.self.ip))
              info("LocalMessageServer: CMD " + cmdID + " - Sent ACKPrepareRemoveHostMessage to message server")
            } else
              error("LocalMessageServer: current cmd, " + cmdID + " is younger than lastCmdID, " + lastCmdID)

          case UpdateRing(cmdID) =>
            info("LocalMessageServer: Received " + msg)
            debug("Received UpdateRing")

            if (runtime.candidateRing != null) {
              out.writeObject(ACKMessage)
              runtime.ring = runtime.candidateRing
              runtime.appStatic.systemHosts = runtime.candidateHostList._2
              runtime.candidateRing = null
              runtime.candidateHostList = null
              runtime.flushSlatesInBufferToQueue
              info("LocalMessageServer: cmdID - " + cmdID + " update ring done")
            } else {
              warn("UpdateRing: candidate ring is null in UpdateRing")
            }

          case StartSourceMessage(sourceName) =>
            info("LocalMessageServer: Received " + msg)
            out.writeObject(ACKMessage)
            runtime.startSource(sourceName)

          case PING() =>
            trace("Received PRING")

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
}
