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

class MessageServerClient(serverHost: String, serverPort: Int, timeout: Long = 2000L) extends Logging {

  def sendMessage(msg: Message): Boolean = synchronized {
    try {
      info("MessageServerClient: send " + msg + " to Message Server: " + serverHost + ", " + serverPort)
      val socket = new Socket(serverHost, serverPort)
      val out = new ObjectOutputStream(socket.getOutputStream)
      val in = new ObjectInputStream(socket.getInputStream)
      info("MessageServerClient: connected")
      out.writeObject(msg)
      val ack = in.readObject
      ack match {
        case AckOfNodeRemove(node) => debug("MessageServerClient: " + ack)
        case AckOfNodeJoin(node) => debug("MessageServerClient: " + ack)
        case _ => error("MessageServerClient error: received wrong message while expecting ACK, " + ack)
      }
      out.close
      in.close
      socket.close
    } catch {
      case e: Exception => error("MessageServerClient sendMessage exception. MSG = " + msg.toString, e); false
    }
    true
  }

}

class LocalMessageServerClient(serverHost: String, serverPort: Int, timeout: Long = 2000L) extends Logging {

  def sendMessage(msg: Message): Boolean = synchronized {
    try {
      info("LocalMessageServerClient: send " + msg + " to server: " + serverHost + ", " + serverPort)
      val socket = new Socket(serverHost, serverPort)
      val out = new ObjectOutputStream(socket.getOutputStream)
      val in = new ObjectInputStream(socket.getInputStream)
      info("LocalMessageServerClient: connected")
      out.writeObject(msg)
      val ack = in.readObject
      ack match {
        case AckOfNewRing(commandId: Int) => info("LocalMessageServerClient: received " + ack)
        case _ => error("MessageServerClient error: received wrong message while expecting ACK, " + ack)
      }
      out.close
      in.close
      socket.close
    } catch {
      case e: Exception => error("LocalMessageServerClient sendMessage exception. MSG = " + msg.toString, e); false
    }
    true
  }

}
