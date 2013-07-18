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
import annotation.tailrec
import grizzled.slf4j.Logging

/**
 *  A map from Int < N to hostname.
 */
class HashRing(val hash: IndexedSeq[String]) extends Logging {
  private val N = hash.size

  // pick up hosts
  def apply(key : Any) : String = {
    hash(((key.hashCode * 997 + Int.MaxValue.toLong) % N).toInt)
  }

  override def toString: String = hash.toString
}

object HashRing2 {
  private val N = 5000
  private val random = new Random(System.currentTimeMillis)

  def initFromHosts(hosts: IndexedSeq[Host]): HashRing2 = {
    def _initFromHosts(hostList: List[Host], ring: HashRing2): HashRing2 = {
      if (hostList.isEmpty) ring
      else if (ring == null) {
        val hash = Vector.range(0, N) map (x => hostList.head)
        val map = Map(hostList.head -> N)
        val newRing = initFromHost(hostList.head)
        _initFromHosts(hostList.tail, newRing)
      } else {
        val newHosts = ring.iPs :+ hostList.head.ip
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

  def initFromHost(host: Host): HashRing2 = synchronized {
    val hash = Vector.range(0, N) map (x => host.ip)
    val map = Map(host -> N)
    new HashRing2(IndexedSeq[String](host.ip), hash, Map(host.ip -> host.hostname), Map(host.ip -> N))
  }
}

// This version of hash ring is used and maintained by message server
// ips: ip list of nodes in cluster
// hash: real hash table, inited with hostlist(0). map each ip to a slot
// ipHostMap: map ip address to hostname
// hostCountMap: (ip -> slot # of ip); stat map for add and remove's convenience
class HashRing2 private (val iPs: IndexedSeq[String], val hash: IndexedSeq[String], val ipHostMap: Map[String, String], val hostCountMap: Map[String, Int]) extends Serializable with Logging {

  /**
   * Remove a target [0, numTargets) (so apply will never return it again).
   * Algo: Find all slots maps to hostToRemove, assign those slots to rest
   * hosts one by one.
   * hostToRemove: ip address of node to remove
   */
  def remove(newIPList: IndexedSeq[String], iPToRemove: String): HashRing2 = synchronized {
    def pickHost(map: Map[String, Int]): String = {
      // randomly pick 2 hosts, use the one existing less in hash to fill slot to be removed
      val h1 = newIPList(HashRing2.random.nextInt(newIPList.size))
      val h2 = newIPList(HashRing2.random.nextInt(newIPList.size))
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

    if (newIPList.isEmpty) null
    else {
      val slotsToReFill = hash.zipWithIndex.filter(_._1.compareTo(iPToRemove) == 0).map(_._2).toList
      val (newHash, newMap) = _remove(slotsToReFill, hash, hostCountMap)
      new HashRing2(newIPList, newHash, ipHostMap - iPToRemove, newMap - iPToRemove)
    }
  }

  /**
   * Add a new host, hostToAdd.
   * Algo: Randomly pick totalSlots/len_of_new_host_list slots
   * and put new host into them
   */
  def add(newIPList: IndexedSeq[String], hostToAdd : Host): HashRing2 = synchronized {
    @tailrec
    def pickSlot(hash: IndexedSeq[String]): Int = {
      val r = HashRing2.random.nextInt(HashRing2.N)
      if (hash(r).compareTo(hostToAdd.ip) == 0) pickSlot(hash) else r
    }

    def _add(numToGo: Int, hash: IndexedSeq[String], map: Map[String, Int]): (IndexedSeq[String], Map[String, Int]) = {
      if (numToGo > 0) {
        // randomly pick 2 slots, and feed hostToAdd into the one with more slots in hash
        val p = (pickSlot(hash), pickSlot(hash))
        val slot = if (map(hash(p._1)) > map(hash(p._2))) p._1 else p._2
        val newMap = map updated (hash(slot), map(hash(slot)) - 1) // also update count map
        val newHash = hash updated (slot, hostToAdd.ip)
        _add(numToGo - 1, newHash, newMap)
      } else (hash, map)
    }

    val totalSlot = HashRing2.N / (iPs.size + 1)
    val (newHash, newMap) = _add(totalSlot, hash, hostCountMap)
    new HashRing2(newIPList, newHash, ipHostMap + (hostToAdd.ip -> hostToAdd.hostname), newMap + (hostToAdd.ip -> totalSlot))
  }

  def stat(hostList: IndexedSeq[String], target: Double): Boolean = {
    val targetN: Int = HashRing2.N / hostList.size
    !hostCountMap.exists(x =>  math.abs(x._2 - targetN) > targetN * target)
  }

  def size = iPs.size

  override def toString(): String = hostCountMap.map(p => (ipHostMap(p._1) -> p._2)).toString

  // equals and hashCode are for unit test
  override def equals(that: Any) : Boolean =
    that.isInstanceOf[HashRing2] && (this.hash == that.asInstanceOf[HashRing2].hash) && (this.iPs == that.asInstanceOf[HashRing2].iPs);

  override def hashCode = hash.hashCode
}
