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

import java.net.Socket
import scala.collection.mutable
import java.io._
import java.util.concurrent.{ArrayBlockingQueue, TimeUnit}
import java.net.{InetAddress,Socket,SocketException}

class MessageServerClient (
    func : String => Unit,
    serverHost : String,
    serverPort : Int,
    timeout : Long = 2000L)
    extends Runnable {

  var lastCmd = 0
  var socket : Socket = null
  var out : OutputStream = null
  var in : BufferedReader = null

  val msgQueue = new ArrayBlockingQueue[String](10)

  def messageDecoder(callback : String => Unit, str : String) = callback(str)

  def addRemoveMessage(host: String): Unit = msgQueue.add(MessageServer.REMOVE_HEADER + host)

  def addAddMessage(host: String) = msgQueue.add(MessageServer.ADD_HEADER + host)

  def run() {
    connect
    while (true) {
      var msg : String = null
      if ({
        msg = msgQueue.poll(timeout, TimeUnit.MILLISECONDS)
        msg != null || in.ready()
      }) {
        if (msg != null) {
          // send msg to message server
          retryUntilSuccess(msg)
        }
        if (in.ready) {
          receiveMessage
        }
      }
    }
    disconnect
  }

  def connect() = {
    try {
      socket = new Socket(serverHost, serverPort)
      out = socket.getOutputStream
      in = new BufferedReader(new InputStreamReader(socket.getInputStream))
    } catch {
      case e : Exception => e.printStackTrace()
    }
  }

  def disconnect() = {
    try {
      in.close
      out.close
      if (socket.isConnected()) socket.close
    } catch {
      case e : Exception => e.printStackTrace()
    }
  }

  def sendMessage (input : String) = {
    try {
      out.write((input+"\n").getBytes)
      out.flush()
    }
    catch {
      case e: IOException =>
        e.printStackTrace()
    }
  }

  def receiveMessage() : String = {

    try {
      var res : String = null
      while ( {
        res = in.readLine
        res != null && res.startsWith("BROADCAST: ")
      }) {
        processBroadcastMessage(res)
      }
      res
    } catch {
      case e: IOException =>
        e.printStackTrace
        "IOException\n"
    }
  }

  def retryUntilSuccess(cmd : String) : Unit = {
    var msg : String = null
    var res : String = null
    do {
      msg = (lastCmd + 1).toString + " " + cmd
      sendMessage(msg)
      res = receiveMessage
      applyMessage(res)
    } while (res.replaceFirst("\\d+\\s", "") != cmd)
  }

  def processBroadcastMessage(msg : String) : Unit = {
    val trimmed = msg.replaceFirst("BROADCAST: ", "")
    messageDecoder(func, trimmed)
  }

  // for now, only update lastCmd #
  def applyMessage(msg : String) : Unit = {
    val tokens = msg.trim.split("[ \t\n]")
    if (tokens(0).toInt < lastCmd + 1) {
      println("warning: something msg is missed, tokens(0) = " + tokens(0) + ", lastCmd = " + lastCmd)
    }
    lastCmd = tokens(0).toInt  // fast forward to the lastCmd provided by server
  }

  def getHosts() : String = {
    sendMessage("0 hosts")
    receiveMessage
  }
}
