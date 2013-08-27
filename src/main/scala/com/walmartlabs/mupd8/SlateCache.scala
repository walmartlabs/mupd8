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

import com.walmartlabs.mupd8.Misc._
import com.walmartlabs.mupd8.GT._
import com.walmartlabs.mupd8.application.binary
import grizzled.slf4j.Logging
import scala.collection._

class SlateValue(val slate: SlateObject, val keyRef: Node[(String, Key)], var dirty: Boolean = true)

class SlateCache(val io: IoPool, val usageLimit: Long, val tls: TLS) extends Logging {
  println("Creating new SlateCache with IoPool " + io + " and usageLimit " + usageLimit);
  private val lock = new scala.concurrent.Lock
  private val table = new mutable.HashMap[(String, Key), SlateValue]()
  private val lru = new DLList[(String, Key)]

  private var currentUsage: Long = 0
  private val objOverhead: Long = 32

  def buildKey(name: String, key: Key) = name + "~~~" + str(key.value)

  def getSlate(key: (String, Key)) = {
    lock.acquire()
    val item = table.get(key)
    item map { p =>
      lru.remove(p.keyRef)
      lru.add(p.keyRef)
    }
    lock.release()
    item.map(_.slate)
  }

  def waitForSlate(key: (String, Key), action: SlateObject => Unit, updater : binary.SlateUpdater, slateBuilder : binary.SlateBuilder, doSafePut : Boolean = true) {
    lock.acquire()
    val item = table.get(key)
    item map { p =>
      lru.remove(p.keyRef)
      lru.add(p.keyRef)
    }
    lock.release()
    item map { p =>
      action(p.slate)
    } getOrElse {
      while (io.pendingCount > 200) java.lang.Thread.sleep(100)
      def slateFromBytes(p:Array[Byte]) = { slateBuilder.toSlate(p) }
      if( doSafePut ) {
        io.fetchSlates(key._1, key._2, p => action(safePut(key, p.map {slateFromBytes}.getOrElse{updater.getDefaultSlate()})))
      } else {
        io.fetchSlates(key._1, key._2, p => action(p.map {slateFromBytes}.getOrElse{updater.getDefaultSlate()}))
      }
    }
  }

  @inline private def bytesConsumed(key : (String, Key), item : SlateObject) = 1

  private def unLockedPut(key: (String, Key), value: SlateObject, dirty: Boolean = true) = {
    val newVal = new SlateValue(value, new Node(key), dirty)
    lru.add(newVal.keyRef)
    currentUsage += bytesConsumed(key, value)
    val oldVal = table.put(key, newVal)
    assert(dirty || oldVal == None)
    oldVal map { p =>
      lru.remove(p.keyRef)
      currentUsage -= bytesConsumed(key, value)
    }

    var count = 0
    while (currentUsage > usageLimit && lru.last != lru.front && count < 10) {
      //      assert(last != null && lru != null)
      val item = lru.last.item
      lru.remove(lru.last)
      table.get(item) map { p =>
        if (p.dirty) {
          // Potential infinite loop here, which is why we are forced to use count
          lru.add(p.keyRef)
        } else {
          currentUsage -= bytesConsumed(key, value)
          table.remove(item)
        }
      } getOrElse {
        assert(false)
      }
      count += 1
    }
    newVal
  }

  // Used only by replaceSlate
  def put(key: (String, Key), value: SlateObject) {
    lock.acquire()
    unLockedPut(key, value)
    lock.release()
  }

  private def safePut(key: (String, Key), value: SlateObject) = {
    lock.acquire()
    val slate = table get (key) getOrElse unLockedPut(key, value, false)
    lock.release()
    slate.slate
  }

  def getDirtyItems(): List[((String, Key), SlateValue)] = {
    lock.acquire()
    val retVal = table.filter(_._2.dirty).toList
    lock.release()
    retVal
  }

  // filter out (string, slatevalue) list which needs to be flushed into cassandra during ring change
  def getFilteredDirtyItems(): List[((String, Key), SlateValue)] = {
    lock.acquire
    val retVal = table.filter(x => if (!x._2.dirty) false
                                   else if (tls.appRun.candidateRing == null) false
                                   else {
                                     // decompose key to build performerpacket key
                                     tls.appRun.self.ip.compareTo(tls.appRun.candidateRing(PerformerPacket.getKey(tls.appRun.appStatic.performerName2ID(x._1._1), x._1._2))) != 0
                                   }).toList
    lock.release
    retVal
  }

  def getAllItems() = {
    lock.acquire()
    val retVal = table.toList
    lock.release()
    retVal
  }

}
