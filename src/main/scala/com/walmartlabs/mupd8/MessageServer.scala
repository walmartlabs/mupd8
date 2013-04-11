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

import java.net.ServerSocket
import java.net.Socket
import java.io._
import java.util.concurrent.Executors
import java.util.concurrent.ExecutorService
import joptsimple._
import scala.collection.mutable
import scala.collection.mutable.Buffer
import com.walmartlabs.mupd8.messaging.MessageParser
import com.walmartlabs.mupd8.messaging.NodeFailureMessage
import com.walmartlabs.mupd8.messaging.NodeJoinMessage
import com.walmartlabs.mupd8.messaging.HostRequestMessage
import com.walmartlabs.mupd8.messaging.NodeStatusReportMessage
import com.walmartlabs.mupd8.messaging.LoadDistInstructMessage
import com.walmartlabs.mupd8.messaging.UpdateRingMessage
import com.walmartlabs.mupd8.messaging.MessageKind

import grizzled.slf4j.Logging

object config {
  val threshold = 100000 // Must be >= 1
  assert(threshold >= 1)
}

/* class to track messages */
class MessageTracker {
  private var lastCmd = 0
  private val cmdQueue = new mutable.Queue[String]

  def propose(newCmd : Int, cmd : String) : Boolean = synchronized {
    // In case one node is shutdown and added back again, cmd # is
    // is reset to 1 so that this queue is not working again for
    // now regardless of cmd#, put all new cmd at the end of queue
    // TODO: remove queue and make message server stateless
    cmdQueue.enqueue(cmd)
    if (cmdQueue.size > config.threshold) cmdQueue.dequeue()
    lastCmd = cmdQueue.size
    true
  }

  def getLastCmd : Option[String] = synchronized {
    if (cmdQueue.size == 0) None else Some(lastCmd.toString + " " + cmdQueue.last)
  }
  
  // XXX: do we need this?
  def getCmd(cmd : Int) : Option[String] = synchronized {
    val index = cmd - lastCmd + cmdQueue.size - 1
    if ((index < 0) || (index >= cmdQueue.size)) None else Some(cmd.toString + " " + cmdQueue(index))
  }

  // XXX: do we need this?
  def lastCmds = synchronized (lastCmd until lastCmd - cmdQueue.size by -1).map(getCmd(_).get).foldLeft("")(_ + _ + "\n")
}

/* class to track live hosts */
class HostTracker {
  val mupd8Hosts = new mutable.HashMap[String, OutputStream] //consider use Socket instead of OutputStream

  def registerHost(host : String, out : OutputStream) : Boolean = synchronized {
    mupd8Hosts += host -> out
    true
  }

  def removeHost(host : String) : Unit = synchronized {
    mupd8Hosts.remove(host)
  }

  // Get all hosts' name into sorted array,
  // and combine them into string formated as ["host.1", "host.2"]
  def getSortedHostsString : String = {
    val hostArr = mupd8Hosts.keySet.toArray.sorted
    if (hostArr.isEmpty) "[]"
    else "[" + hostArr.tail.foldLeft("\""+hostArr.head+"\"")((x, y) => x + ", \""+y+"\"") + "]"
  }

  def broadcast(message : String) = synchronized {
    val msg = "BROADCAST: " + message
    mupd8Hosts.foreach((v) =>  {
        try {
          v._2.write(msg.getBytes)
          v._2.flush
        } catch { case e : IOException => e.printStackTrace() }
      })
  }
  
   // COMMENT: used to send message directly to a specific host, e.g. planner
  def unicast(host: String, msgContent: String) = {
    var ipAddress = Misc.getIPAddress(host)
    if (Misc.isLocalHost(host)) {
      ipAddress = "127.0.0.1"
    }
    
    mupd8Hosts.filter(_._1.equals(ipAddress)).foreach((entry) => {
      try {
        entry._2.write(msgContent.getBytes())
        entry._2.flush
      } catch {
        case e: IOException => e.printStackTrace()
      }
    })
  }

  override def toString() : String = synchronized {
    mupd8Hosts.keySet.toString
  }
}

/* socket server to communicate clients */
class MessageServerThread(val port : Int) extends Logging {

  val hostTracker = new HostTracker
  val msgTracker = new MessageTracker
  val pool : ExecutorService = Executors.newCachedThreadPool()
  var ssocket : ServerSocket = null
  def run() = {
    info("Message server attempts to listen to port: " + port)
    ssocket = new ServerSocket(port)
    info("Message server started, listening at port: " + port)
    try {
      while (true) {
        val csocket = ssocket.accept()
        pool.execute(new RequestHandler(csocket, hostTracker, msgTracker))
      }
    } catch { case e : Exception => error(e.getMessage) }
  }

  def shutdown() = {
    info("Message server shutting down...")
    try {
      ssocket.close
      pool.shutdown
    } catch { case e : Exception => {}}
  }
  
  def daemonize() = {
    System.in.close()
    Runtime.getRuntime().addShutdownHook( new Thread { override def run() = MessageServerThread.this.shutdown()})
  }
}

/* accepted socket end to handle client requests */
class RequestHandler(
  val socket : Socket,
  val hostTracker : HostTracker,
  val msgTracker : MessageTracker) extends Runnable with Logging {

  override def run() = {
    val in = new BufferedReader(new InputStreamReader(socket.getInputStream))
    val out = socket.getOutputStream
    val hostAddr : String = socket.getInetAddress.getHostAddress()
    val port : Int = socket.getPort()
    hostTracker.registerHost(hostAddr, out)
    info("Registered host: " + hostAddr)
    try {
      var line : Option[String] = None
      while ({
          line = Misc.excToOption(in.readLine)
          line != None
        }) {
        cmdResponse(line.get)
      }
    } catch {
      case e : Exception => {
        error("Broken pipe: " + hostAddr)

        // Remove hostAddr from hostTracker
        hostTracker.removeHost(hostAddr)
        // broadcast new host list to all nodes
        // cmdNo is generated by msgTracker in this case
        //if (msgTracker.propose(-1, MessageServer.REMOVE_HEADER + hostAddr)) {
        //  hostTracker.broadcast(msgTracker.getLastCmd.get + "\n")
        //}
        var mesg = constructMessage(MessageKind.NODE_FAILURE,hostAddr)
        hostTracker.broadcast(mesg)
        in.close
        out.close
        socket.close
      }
    }
  }

  def constructMessage(msgKind: MessageKind.Value, msgContent: String): String = {
     MessageKind.MessageBegin + msgKind + ":" + msgContent.toString() + "\n"
  }

 /*
 COMMENT: Construct response to a message received by the MessageServer. 
 */  
   def cmdResponse(cmd: String): Unit = {

    MessageParser.getMessage(cmd) match {
      case msg: NodeFailureMessage =>
        var mesg = constructMessage(MessageKind.NODE_FAILURE,msg.getFailedNodeName())
        hostTracker.removeHost(msg.getFailedNodeName())
        hostTracker.broadcast(mesg)
      case msg: NodeJoinMessage => hostTracker.broadcast(constructMessage(MessageKind.NODE_JOIN, msg.toString()))
      case msg: HostRequestMessage =>
        var hostlist = ""
        var hostArray = hostTracker.mupd8Hosts.keySet.toArray.sorted
        for ((x, i) <- hostArray.view.zipWithIndex) {
          hostlist += hostArray(i)
          hostlist += ","
        }
        var mesg = constructMessage(MessageKind.HOST_LIST, hostlist)
        hostTracker.unicast((msg.asInstanceOf[HostRequestMessage]).getSender(), mesg)
      case msg: NodeStatusReportMessage =>
        info("Received message: " + msg)
        var mesg = constructMessage(MessageKind.NODE_STATUS_REPORT, msg.toString())
        hostTracker.unicast((msg.asInstanceOf[NodeStatusReportMessage]).getRecipient, mesg)
      case msg: LoadDistInstructMessage =>
        info("Received request for redistribution: " + msg)
        var mesg = constructMessage(MessageKind.LOAD_DIST_INSTRUCT, msg.toString())
        hostTracker.unicast((msg.asInstanceOf[LoadDistInstructMessage]).getDestHost, mesg)
        hostTracker.unicast((msg.asInstanceOf[LoadDistInstructMessage]).getSourceHost, mesg)
      case msg: UpdateRingMessage =>
        info("Changing ring: " + msg)
        hostTracker.broadcast(constructMessage(MessageKind.UPDATE_RING, msg.toString()))
      case others => error("Syntax error for message: " + others)
    }
    null

  }
  
  
  
  
}

object MessageServer extends Logging {

  def main(args: Array[String]) {
    val parser = new OptionParser
    val folderOpt  = parser.accepts("d", "REQUIRED: config file folder containing sys and app configs")
                           .withRequiredArg().ofType(classOf[String])
    val sysOpt     = parser.accepts("s", "DEPRECATED: sys config file")
                           .withRequiredArg().ofType(classOf[String])
    val appOpt     = parser.accepts("a", "DEPRECATED: app config file")
                           .withRequiredArg().ofType(classOf[String])
    val pidOpt     = parser.accepts("pidFile", "mupd8 process PID file")
                           .withRequiredArg().ofType(classOf[String]).defaultsTo("messageserver.pid")
    val options = parser.parse(args : _*)
    
    var config : application.Config = null
    if (options.has(folderOpt)) {
      info("Config file folder " + folderOpt + " is provided")
      config = new application.Config(new File(options.valueOf(folderOpt)))
    }
    else if (options.has(sysOpt) && options.has(appOpt)) {
      info("Sys config file " + sysOpt + " and app config file " + appOpt + " are provided")
      config = new application.Config(options.valueOf(sysOpt), options.valueOf(appOpt))
    }
    else {
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
    }
    else {
      info(host.get + " is not a message server host. Quiting...")
      System.exit(0);
    }
  }
}
