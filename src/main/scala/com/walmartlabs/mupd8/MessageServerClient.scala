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
      case e: Exception => error("MessageServerClient sendMessage exception. MSG = " + msg.toString, e)
      false
    }
  }

}

class LocalMessageServerClient(serverHost: String, serverPort: Int, timeout: Int = 2000) extends Logging {

  def sendMessage(msg: Message): Boolean = synchronized {
    try {
      info("LocalMessageServerClient: send " + msg + " to server: " + serverHost + ", " + serverPort)
      val socket = new Socket(serverHost, serverPort)
      val out = new ObjectOutputStream(socket.getOutputStream)
      socket.setSoTimeout(timeout)
      info("LocalMessageServerClient: connected")
      out.writeObject(msg)
      out.close
      socket.close
      true
    } catch {
      case e: Exception => error("LocalMessageServerClient sendMessage exception. MSG = " + msg.toString + ", dest = " + (serverHost , serverPort), e)
      false
    }
  }

}
