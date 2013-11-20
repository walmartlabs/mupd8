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
import java.net.{ InetAddress, Socket, SocketException }
import java.io.FileInputStream
import java.io.FileOutputStream
import java.io.IOException
import java.io.ObjectInputStream
import java.io.ObjectOutputStream
import grizzled.slf4j.Logging

class MessageServerClient(serverHost: String, serverPort: Int, timeout: Int = 2000) extends Logging {

  def sendMessage(msg: Message): Boolean = synchronized {
    def _sendMessage(retryCount: Int, msg: Message): Boolean = {
      if (retryCount == 0) {
        false
      } else {
        try {
          debug("MessageServerClient: send " + msg + " to Message Server: " + serverHost + ", " + serverPort)
          val socket = new Socket(serverHost, serverPort)
          val out = new ObjectOutputStream(socket.getOutputStream)
          val in = new ObjectInputStream(socket.getInputStream)
          socket.setSoTimeout(timeout)
          info("MessageServerClient: connected")
          out.writeObject(msg)
          msg match {
            case m: MessageWOACK =>

            case m: MessageWACK =>
              val ack = in.readObject
              info("MessageServerClient: received " + ack)
          }
          out.close
          in.close
          socket.close
          true
        } catch {
          case e: Exception =>
            error("MessageServerClient sendMessage exception. MSG = " + msg.toString, e)
            _sendMessage(retryCount - 1, msg)
        }
      }
    }

    _sendMessage(6, msg)
  }

  // check host ip address and hostname by connecting message server
  def checkIP(): Host = {
    def getHostName(retryCount: Int): Host = {
      if (retryCount > 10) {
        Host(InetAddress.getLocalHost.getHostAddress, InetAddress.getLocalHost.getHostName)
      } else {
        try {
          val s = new java.net.Socket(serverHost, serverPort)
          val host = Host(s.getLocalAddress.getHostAddress, s.getLocalAddress.getHostName)
          val out = new ObjectOutputStream(s.getOutputStream)
          out.writeObject(IPCHECKDONE)
          out.close
          s.close
          host
        } catch {
          case e: Exception => warn("MessageServerClient::checkIP - Connect to message server failed, retry", e); getHostName(retryCount + 1)
        }
      }
    }

    getHostName(0)
  }

}

class LocalMessageServerClient(serverHost: String, serverPort: Int, timeout: Int = 2000) extends Logging {

  def sendMessage(msg: Message): Boolean = synchronized {
    def _sendMessage(retryCount: Int, msg: Message): Boolean = {
      if (retryCount == 0) {
        false
      } else {
        try {
          msg match {
            case PING() => trace("LocalMessageServerClient: send " + msg + " to server: " + serverHost + ", " + serverPort)
            case _ => info("LocalMessageServerClient: send " + msg + " to server: " + serverHost + ", " + serverPort)
          }
          val socket = new Socket(serverHost, serverPort)
          val out = new ObjectOutputStream(socket.getOutputStream)
          val in = new ObjectInputStream(socket.getInputStream)
          socket.setSoTimeout(timeout)
          trace("LocalMessageServerClient: connected")
          out.writeObject(msg)
          msg match {
            case m: MessageWOACK =>

            case m: MessageWACK =>
              val ack = in.readObject
              info("LocalMessageServerClient: received " + ack)
          }
          in.close
          out.close
          socket.close
          true
        } catch {
          case e: Exception =>
            error("LocalMessageServerClient sendMessage exception. MSG = " + msg.toString + ", dest = " + (serverHost, serverPort) + " with retryCount = " + retryCount, e )
            Thread.sleep(10000)
            _sendMessage(retryCount - 1, msg)
        }
      }
    }

    _sendMessage(6, msg)
  }
}
