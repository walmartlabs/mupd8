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

import scala.util.Random
import scala.math.floor
import scala.collection.mutable.ArrayBuffer
import scala.collection.immutable.Map
import grizzled.slf4j.Logging

/**
 *  A map from Int < N to hostname.
 */
class HashRing(val hash: IndexedSeq[String]) extends Logging {
  private val N = hash.size

  // pick up hosts
  def apply(key : Any) : String = {
    hash(((key.hashCode + Int.MaxValue.toLong) % N).toInt)
  }

  override def toString: String = hash.toString
}

object HashRing2 {
  private val N = 2000
  private val random = new Random(System.currentTimeMillis)

  def initFromHosts(hosts: IndexedSeq[String]): HashRing2 = {
    def _initFromHosts(hostList: List[String], ring: HashRing2): HashRing2 = {
      if (hostList.isEmpty) ring
      else if (ring == null) {
        val hash = Vector.range(0, N) map (x => hostList.head)
        val map = Map(hostList.head -> N)
        val newRing = initFromHost(hostList.head)
        _initFromHosts(hostList.tail, newRing)
      } else {
        val newHosts = ring.hosts :+ hostList.head
        val newRing = ring.add(newHosts, hostList.head)
        _initFromHosts(hostList.tail, newRing)
      }
    }

    if (hosts.isEmpty) null
    else {
      val hostList = hosts.toList
      _initFromHosts(hostList, null)
    }
  }

  def initFromHost(host: String): HashRing2 = synchronized {
    val hash = Vector.range(0, N) map (x => host)
    val map = Map(host -> N)
    new HashRing2(Vector[String](host), hash, map)
  }
}

// This version of hash ring is used and maintained by message server
// hostList: host list
// hash: real hash table, inited with hostlist(0)
// map: stat map for add and remove's convenience
class HashRing2 private (val hosts: IndexedSeq[String], val hash: IndexedSeq[String], map: Map[String, Int]) extends Serializable with Logging {

  def getCopyOfHash: IndexedSeq[String] = hash.toIndexedSeq

  /** Map from key [0.0, 1.0] to an unremoved target [0, numTargets). */
  def apply(key : Int) : String = {
    val offset = key * 997 / HashRing2.N
    hash(offset)
  }

  /**
   * Remove a target [0, numTargets) (so apply will never return it again).
   * Algo: Find all slots maps to hostToRemove, assign those slots to rest
   * hosts one by one.
   */
  def remove(newHostList: IndexedSeq[String], hostToRemove: String): HashRing2 = synchronized {
    def pickHost(map: Map[String, Int]): String = {
      // randomly pick 2 hosts, use the one existing less in hash to fill slot to be removed
      val h1 = newHostList(HashRing2.random.nextInt(newHostList.size))
      val h2 = newHostList(HashRing2.random.nextInt(newHostList.size))
      if (map(h1) > map(h2)) h2 else h1
    }

    def _remove(slots: List[Int], hash: IndexedSeq[String], map: Map[String, Int]): (IndexedSeq[String], Map[String, Int]) = {
      if (slots.isEmpty) (hash, map)
      else {
        val h = pickHost(map)
        val newHash = hash updated (slots.head, h)
        val newMap = map updated (h, map(h) + 1)
        _remove(slots.tail, newHash, newMap)
      }
    }

    if (newHostList.isEmpty) null
    else {
      val slotsToReFill = hash.zipWithIndex.filter(_._1.compareTo(hostToRemove) == 0).map(_._2).toList
      val (newHash, newMap) = _remove(slotsToReFill, hash, map)
      new HashRing2(newHostList, newHash, newMap - hostToRemove)
    }
  }

  /**
   * Add a new host, hostToAdd.
   * Algo: Randomly pick totalSlots/len_of_new_host_list slots
   * and put new host into them
   */
  def add(newHostList: IndexedSeq[String], hostToAdd : String): HashRing2 = synchronized {
    def pickSlot(hash: IndexedSeq[String]): Int = {
      val r = HashRing2.random.nextInt(HashRing2.N)
      if (hash(r).compareTo(hostToAdd) == 0) pickSlot(hash) else r
    }

    def _add(numToGo: Int, hash: IndexedSeq[String], map: Map[String, Int]): (IndexedSeq[String], Map[String, Int]) = {
      if (numToGo > 0) {
        // randomly pick 2 slots, and feed hostToAdd into the one with more slots in hash
        val p = (pickSlot(hash), pickSlot(hash))
        val slot = if (map(hash(p._1)) > map(hash(p._2))) p._1 else p._2
        val newMap = map updated (hash(slot), map(hash(slot)) - 1) // also update count map
        val newHash = hash updated (slot, hostToAdd)
        _add(numToGo - 1, newHash, newMap)
      } else (hash, map)
    }

    val totalSlot = HashRing2.N / newHostList.size
    val (newHash, newMap) = _add(totalSlot, hash, map)
    new HashRing2(newHostList, newHash, newMap + (hostToAdd -> totalSlot))
  }

  def stat(hostList: IndexedSeq[String], target: Double): Boolean = {
    val targetN: Int = HashRing2.N / hostList.size
    !map.exists(x =>  math.abs(x._2 - targetN) > targetN * target)
  }

  def size = hosts.size

  override def toString(): String = map.toString

  // equals and hashCode are for unit test
  override def equals(that: Any) : Boolean =
    that.isInstanceOf[HashRing2] && (this.hash == that.asInstanceOf[HashRing2].hash) && (this.hosts == that.asInstanceOf[HashRing2].hosts);

  override def hashCode = hash.hashCode
}
