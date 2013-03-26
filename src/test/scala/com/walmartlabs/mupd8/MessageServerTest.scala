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
import scala.util.Random
import scala.collection.JavaConverters._

@RunWith(classOf[JUnitRunner])
class MessageServerTest extends FunSuite {

  test("MessageServer/Client add/remove") {
    val random = new Random(System.currentTimeMillis)

    val server = new MessageServer.MessageServerThread(4568, true)
    val sThread = new Thread(server, "Message Server Thread")
    sThread.start
    val client = new MessageServerClient("localhost", 4568, 50L)
    Thread.sleep(2000)
    val nodes = Vector.range(0, 5) map (i => "machine" + (random.nextInt(10) + i * 10) + ".example.com")
    for (node <- nodes) client.sendMessage(NodeJoinMessage(node))
    Thread.sleep(1000)
    assert(MessageServer.ring2.size == 5)
    for (node <- nodes) client.sendMessage(NodeRemoveMessage(node))
    Thread.sleep(1000)
    assert(MessageServer.ring2 == null)
    assert(MessageServer.lastCmdID == 10)
    assert(MessageServer.lastCmdID != 11)
    server.shutdown
    println("MessageServer Test is done")
  }

  test("LocalMessageServer/Client") {
  }

}

