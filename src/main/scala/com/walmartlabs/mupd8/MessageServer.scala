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
import java.net.ServerSocket

/* Message Server for whole cluster */
object MessageServer extends Logging {

  /* socket server to communicate clients */
  // In unit test skip sending new ring to all nodes
  class MessageServerThread(val port : Int, val isTest: Boolean = false) extends Runnable {

    var keepRunning = true
    var currentThread: Thread = null
    override def run(): Unit = {
      info("MessageServerThread: Start listening to :" + port)
      val serverSocketChannel = ServerSocketChannel.open()
      serverSocketChannel.socket().bind(new InetSocketAddress(port))
      debug("server started, listening " + port)

      // Incoming messages need to be processed sequentially
      // since it might cause hash ring accordingly. So NOT
      // generate one thread for each incoming request.
      while (keepRunning) {
        try {
          currentThread = Thread.currentThread
          val channel = serverSocketChannel.accept()
          debug("Channel - " + channel + " is opened")
          val in = new ObjectInputStream(Channels.newInputStream(channel))
          val out = new ObjectOutputStream(Channels.newOutputStream(channel))
          val msg = in.readObject()
          msg match {
            case NodeRemoveMessage(node) =>
              info("MessageServer: received node remove message: " + msg)
              lastCmdID += 1
              lastRingUpdateCmdID  = lastCmdID
              // send ACK to reported
              out.writeObject(AckOfNodeRemove(node))
              // update hash ring
              val newHostList = ring2.hosts filter (host => host.compareTo(node) != 0)
              ring2 = ring2.remove(newHostList, node)
              if (!isTest) {
                // if it is unit test, don't send new ring to all nodes
                // Local message server's port is always port + 1
                ring2.hosts foreach (host => SendNewRing ! (lastCmdID, host, (port + 1), RemoveHostMessage(lastCmdID, node, ring2.hash, ring2.hosts), port))
              }
            case NodeJoinMessage(node) =>
              info("MessageServer: Received node join message: " + msg)
              lastCmdID += 1
              lastRingUpdateCmdID = lastCmdID
              // send ACK to reported
              out.writeObject(AckOfNodeJoin(node))
              // update hash ring
              ring2 = if (ring2 == null) {
                HashRing2.initFromHost(node)
              } else {
                val newHostList = ring2.hosts :+ node
                ring2.add(newHostList, node)
              }
              if (!isTest) {
                // if it is unit test, don't send new ring to all nodes
                // Local message server's port is always port + 1
                ring2.hosts foreach {host =>
                  info("Sending " + ring2 + " to " + host)
                  SendNewRing ! (lastCmdID, host, port + 1, AddHostMessage(lastCmdID, node, ring2.hash, ring2.hosts), port)}
              }
            case _ => error("CmdResponse error: not a valid msg: " + msg)
          }
          out.close; in.close; channel.close
        }  catch {
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
        currentThread.interrupt
        Thread.sleep(2000)
      } catch { case e : Exception => {}}
    }

    def daemonize() = {
      System.in.close()
      Runtime.getRuntime().addShutdownHook( new Thread { override def run = shutdown()})
    }

  }

  /* actor sending new ring to nodes in cluster */
  object SendNewRing extends Actor {
    def act() {
      react {
        // msport: port of message server
        case (cmdID: Int, endpointHost: String, endpointPort: Int, msg: Message, msport: Int) =>
          if (cmdID == lastRingUpdateCmdID) {
            // it is still latest command and hash ring
            info("SendNewRing: Sending cmdID " + cmdID + ", msg = " + msg + " to endpoint " + (endpointHost, endpointPort))
            val client = new LocalMessageServerClient(endpointHost, endpointPort)
            if (!client.sendMessage(msg)) {
              // report host fails if any exception happens
              info("SendNewRing: report " + endpointHost + " fails")
              val msClient = new MessageServerClient("localhost", msport)
              msClient.sendMessage(NodeRemoveMessage(endpointHost))
            }
          } else
            info("SendNewRing: skip non-currernt command - " + lastRingUpdateCmdID + ", " + ring2)
          act()
        case "EXIT" =>
          info("SendNewRing receive EXIT")
        case msg =>
          error("MSG - " + msg + "is not supported now")
      }
    }
  }
  SendNewRing.start

  // separate lastCmdID and lastRingUpdateCmdID in case in future
  // there may message not requiring no new ring update
  var lastCmdID = -1
  var lastRingUpdateCmdID = -1
  var ring2: HashRing2 = null // TODO: find a way to init hash ring2

  def main(args: Array[String]) {
    val parser = new OptionParser
    val folderOpt  = parser.accepts("d", "REQUIRED: config file folder containing sys and app configs").withRequiredArg().ofType(classOf[String])
    val sysOpt     = parser.accepts("s", "DEPRECATED: sys config file").withRequiredArg().ofType(classOf[String])
    val appOpt     = parser.accepts("a", "DEPRECATED: app config file").withRequiredArg().ofType(classOf[String])
    val pidOpt     = parser.accepts("pidFile", "mupd8 process PID file").withRequiredArg().ofType(classOf[String]).defaultsTo("messageserver.pid")
    val options = parser.parse(args : _*)

    var config : application.Config = null
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
    val host = Option(config.getScopedValue(Array("mupd8", "messageserver", "host")))
    val port = Option(config.getScopedValue(Array("mupd8", "messageserver", "port")))

    if (options.has(pidOpt)) Misc.writePID(options.valueOf(pidOpt))

    if (Misc.isLocalHost(host.get.asInstanceOf[String])) {
      val server = new MessageServerThread(port.get.asInstanceOf[Number].intValue())
      server.daemonize
      server.run
    } else {
      info("It is not a message server host, quit...")
      System.exit(0);
    }
  }
}

/* Message Server for every node, which receives ring update message for now */
class LocalMessageServer(port: Int, runtime: AppRuntime) extends Runnable with Logging {
  private var lastCmdID = -1;

  override def run() {
    info("LocalMessageServerThread: Start listening to :" + port)
    val serverSocketChannel = ServerSocketChannel.open()
    serverSocketChannel.socket().bind(new InetSocketAddress(port))
    info("local message server started, listening " + port)
    while (true) {
      try {
        val channel = serverSocketChannel.accept()
        val in = new ObjectInputStream(Channels.newInputStream(channel))
        val out = new ObjectOutputStream(Channels.newOutputStream(channel))
        val msg = in.readObject
        info("LocalMessageServer: Received " + msg)
        msg match {
          case AddHostMessage(cmdID, hostToAdd, hash, hosts) => {
            if (cmdID > lastCmdID) {
              runtime.ring = hash
              runtime.app.systemHosts = hosts
              if (runtime.pool != null) runtime.pool.cluster.addHost(hostToAdd)
              out.writeObject(AckOfNewRing(cmdID))
              lastCmdID = cmdID
              info("LocalMessageServer: CMD " + cmdID + " - Update Ring with " + hosts)
            } else
              error("LocalMessageServer: current cmd, " + cmdID + " is younger than lastCmdID, " + lastCmdID)
          }
          case RemoveHostMessage(cmdID, hostToRemove, hash, hosts) => {
            if (cmdID > lastCmdID) {
              runtime.ring = hash
              runtime.app.systemHosts = hosts
              if (runtime.pool != null) runtime.pool.cluster.removeHost(hostToRemove)
              out.writeObject(AckOfNewRing(cmdID))
              lastCmdID = cmdID
              info("LocalMessageServer: CMD " + cmdID + " - Update Ring with " + hosts)
            } else
              error("LocalMessageServer: current cmd, " + cmdID + " is younger than lastCmdID, " + lastCmdID)
          }
          case _ => error("LocalMessageServer error: Not a valid msg, " + msg.toString)
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
