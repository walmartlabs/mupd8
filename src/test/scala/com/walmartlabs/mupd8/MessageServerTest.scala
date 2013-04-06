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

import org.junit.runner.RunWith
import org.junit.Assert._
import org.scalatest.junit.JUnitRunner
import org.scalatest.FunSuite
import java.util.ArrayList
import scala.util.Random
import scala.collection.JavaConverters._
import com.walmartlabs.mupd8.messaging.MessageHandler
import com.walmartlabs.mupd8.messaging.Message
import com.walmartlabs.mupd8.messaging.NodeFailureMessage
import com.walmartlabs.mupd8.messaging.NodeJoinMessage

@RunWith(classOf[JUnitRunner])
class MessageServerTest extends FunSuite {
  
  test("MessageServer/Client update remove") {
    
    val random = new Random(0)
    val array = new ArrayList[String]
    val messageHandler = new CustomMessageHandler(array)
    
    val server = new MessageServerThread(4568)
    val sThread = new Thread(Misc.run(server.run))
    sThread.start
    Thread.sleep(500)
    val client = new MessageServerClient(messageHandler, "localhost", 4568, 50L)
    val t = new Thread(client)
    t.start
    Thread.sleep(2000)
    for ( i <- 0 until 5){
      val failedHost = "machine" + random.nextInt(10).toString + ".example.com"
      val mesg = new NodeFailureMessage(failedHost)
      client.sendMessage(mesg)
    }

    for ( i <- 0 until 5){
      val joiningHost = "machine" + random.nextInt(10).toString + ".example.com"
      val mesg = new NodeJoinMessage(joiningHost)
      client.sendMessage(mesg)
    }

    // give up cpu for client/server to process msgs

    Thread.sleep(2000)
    println("storemsg arr size = " + array.size())
    
    assert(array.size === 10, "# of received msg is wrong")
    println("MessageServer Test is done")
  }

}

class CustomMessageHandler(array: ArrayList[String]) extends MessageHandler {
 
  var ring:HashRing=null

  override def initialize(ring:HashRing) = {
    this.ring=ring
  }


  def actOnMessage(message: Message): Unit = {
    println("recevied message: " + message.getKind + "_" + message.toString)
    array.add(message.toString())
  }
}
