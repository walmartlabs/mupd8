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
import java.net.InetAddress

@RunWith(classOf[JUnitRunner])
class MessageServerTest extends FunSuite {

  test("MessageServer/Client add/remove") {
    val random = new Random(System.currentTimeMillis)

    val server = new MessageServer(null, 4568, Map.empty, true)
    server.start
    Thread.sleep(500)

    val client = new MessageServerClient(null, 50, Host(InetAddress.getByName("localhost").getHostAddress, "localhost"), 4568)
    Thread.sleep(2000)
    val nodes = Vector.range(0, 5) map (i => Host("192.168.1." + i.toString, "machine" + (random.nextInt(10) + i * 10) + ".example.com"))
    for (node <- nodes) client.sendMessage(NodeChangeMessage(Set(node), Set.empty))
    Thread.sleep(1000)
    assert(server.ring.size == 5)
    for (node <- nodes) client.sendMessage(NodeChangeMessage(Set.empty, Set(node)))
    Thread.sleep(1000)
    assert(server.ring == null)
    assert(server.lastCmdID == 10)
    assert(server.lastCmdID != 11)
    server.shutdown
    println("MessageServer Test is done")
  }

}

