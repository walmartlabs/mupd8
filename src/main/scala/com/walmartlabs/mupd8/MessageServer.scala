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

  override def toString() : String = synchronized {
    mupd8Hosts.keySet.toString
  }
}

/* socket server to communicate clients */
class MessageServerThread(val port : Int) {

  val hostTracker = new HostTracker
  val msgTracker = new MessageTracker
  val pool : ExecutorService = Executors.newCachedThreadPool()
  var ssocket : ServerSocket = null
  def run() = {
    ssocket = new ServerSocket(port)
    println("server started, listening " + port)
    try {
      while (true) {
        val csocket = ssocket.accept()
        pool.execute(new RequestHandler(csocket, hostTracker, msgTracker))
      }
    } catch { case e : Exception => println(e.getMessage) }
  }

  def shutdown() = {
    println("Initiate shutdown")
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
  val msgTracker : MessageTracker) extends Runnable {

  override def run() = {
    val in = new BufferedReader(new InputStreamReader(socket.getInputStream))
    val out = socket.getOutputStream
    val hostAddr : String = socket.getInetAddress.getHostAddress()
    val port : Int = socket.getPort()
    hostTracker.registerHost(hostAddr, out)
    println("register " + hostAddr)
    try {
      var line : Option[String] = None
      while ({
          line = Misc.excToOption(in.readLine)
          line != None
        }) {
        out.write(cmdResponse(line.get, out).getBytes)
        out.flush
      }
    } catch {
      case e : Exception => {
        println("broken pipe " + hostAddr)

        // Remove hostAddr from hostTracker
        hostTracker.removeHost(hostAddr)
        // broadcast new host list to all nodes
        // cmdNo is generated by msgTracker in this case
        if (msgTracker.propose(-1, MessageServer.REMOVE_HEADER + hostAddr)) {
          hostTracker.broadcast(msgTracker.getLastCmd.get + "\n")
        }

        in.close
        out.close
        socket.close
      }
    }
  }

  def cmdResponse(cmd : String, out : OutputStream) : String = {
    // cmd format: "update remove/add " + host
    println("#" + cmd)
    val tokens = cmd.trim.split("[ \n\t]")
    val cmdNo = Misc.excToOption(tokens(0).toInt)
    var optResp : Option[String] = None
    if (cmdNo.get == 0 || (cmdNo.get > 0 && tokens(1) == "update")) {
      optResp = tokens(1) match {
        case "update" => {
          if (tokens(2) == "remove" || tokens(2) == "source") {
            if (msgTracker.propose(cmdNo.get, tokens.toList.tail.reduceLeft(_+ " " + _))) {
              hostTracker.broadcast(msgTracker.getLastCmd.get + "\n")
              if (tokens(2) == "remove") hostTracker.removeHost(tokens(3))
            }
            val res = msgTracker.getLastCmd
            if (res == None) {
              Some("CmdNo too High or too Old")
            } else {
              res
            }
          } else if (tokens(2) == "add") {
            // broadcasting new host list as string ["host.1", "host.2"]
            if (msgTracker.propose(cmdNo.get, "update add " + hostTracker.getSortedHostsString)) {
              println("broadcasting : " + msgTracker.getLastCmd.get)
              hostTracker.broadcast(msgTracker.getLastCmd.get + "\n")
            }
            // need to return cmd to confirm with client msg is processed
            Some(cmd)
          } else {
            Some("Unrecognized update command")
          }
        }
        case "hosts" => Some(hostTracker.toString)
        case _       => None
      }
    }
    optResp.getOrElse("Syntax error") + "\n"
  }
}

object MessageServer {
  val REMOVE_HEADER = "update remove "
  val ADD_HEADER = "update add "

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
      System.out.println(folderOpt + " is provided")
      config = new application.Config(new File(options.valueOf(folderOpt)))
    }
    else if (options.has(sysOpt) && options.has(appOpt)) {
      System.out.println(sysOpt + " and " + appOpt + " are provided")
      config = new application.Config(options.valueOf(sysOpt), options.valueOf(appOpt))
    }
    else {
      System.err.println("Missing arguments: Please provide either " + 
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
      System.out.println(host.get + " is not a message server host, quit...")
      System.exit(0);
    }
  }
}
