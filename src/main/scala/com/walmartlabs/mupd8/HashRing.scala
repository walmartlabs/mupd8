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
object HashRing extends Logging {
  val N = 5000
  private val random = new Random(System.currentTimeMillis)

  def initFromHosts(hosts: IndexedSeq[Host]): HashRing = {
    if (hosts.size == 1) {
      HashRing.initFromHost(hosts.head)
    } else {
      val seedRing = HashRing.initFromHost(hosts.head);
      seedRing.add(hosts.tail.toSet)
    }
  }

  def initFromHost(host: Host): HashRing = synchronized {
    val hash = Vector.range(0, N) map (x => host.ip)
    val map = Map(host -> N)
    new HashRing(IndexedSeq[String](host.ip), hash, Map(host.ip -> host.hostname), Map(host.ip -> N))
  }

  def initFromParameters(iPs: IndexedSeq[String], hash: IndexedSeq[String], ipHostMap: Map[String, String]): HashRing = {
    hash.count(ip1 => ip1.compareToIgnoreCase("test") == 0)
    val countMap = hash.distinct.map(ip => (ip, hash.count(ip1 => ip1.compareToIgnoreCase(ip) == 0))).toMap
    new HashRing(iPs, hash, ipHostMap, countMap)
  }

  def initFromRing(ring: HashRing): HashRing =
    new HashRing(ring.ips, ring.hash, ring.ipHostMap, ring.ipCountMap)
}

// This version of hash ring is used and maintained by message server
// ips: ip list of nodes in cluster
// hash: real hash table, inited with hostlist(0). map each ip to a slot
// ipHostMap: map ip address to hostname
// ipCountMap: (ip -> slot # of ip); stat map for add and remove's convenience
class HashRing private (val ips: IndexedSeq[String], val hash: IndexedSeq[String], val ipHostMap: Map[String, String], val ipCountMap: Map[String, Int]) extends Serializable with Logging {
  // pick up hosts
  def apply(key : Any): String = {
    hash(((key.hashCode * 997 + Int.MaxValue.toLong) % HashRing.N).toInt)
  }

  def remove(hosts_to_remove: Set[Host]): HashRing = {
    // remove items in slot_to_remove
    @tailrec
    def removeIP(slots_to_remove: IndexedSeq[Int], hash: IndexedSeq[String], map: Map[String, Int]): (IndexedSeq[String], Map[String, Int]) = {
      @tailrec
      def pickSlot(hash: IndexedSeq[String]): Int = {
        val r = HashRing.random.nextInt(HashRing.N)
        if (hash(r).compareTo(hash(slots_to_remove.head)) == 0) pickSlot(hash) else r
      }

      if (slots_to_remove.isEmpty) {
        (hash, map)
      } else {
        // randomly pick 2 slots, and feed hostToAdd into the one with more slots in hash
        val p = (pickSlot(hash), pickSlot(hash))
        val slot = if (map(hash(p._1)) < map(hash(p._2))) p._1 else p._2
        val newMap = map updated (hash(slot), map(hash(slot)) + 1) // also update count map
        val newHash = hash updated (slots_to_remove.head, hash(slot))
        removeIP(slots_to_remove.tail, newHash, newMap)
      }
    }

    // add ips, return (new hash array, ip count map)
    @tailrec
    def _removeIPs(ips_to_remove: Set[String], hash: IndexedSeq[String], map: Map[String, Int]): (IndexedSeq[String], Map[String, Int]) = {
      if (ips_to_remove.isEmpty) {
        (hash, map)
      } else if (ips.size == 1 && ips_to_remove.size == 1 && ips_to_remove.head.compareTo(ips.head) == 0) {
        (IndexedSeq.empty, Map.empty)
      } else {
        val ip_to_remove = ips_to_remove.head
        if (this.ips.contains(ip_to_remove)) {
          val slots_to_remove = hash.zipWithIndex.filter(p => p._1.compareTo(ip_to_remove) == 0).map(_._2)
          val (newhash, newmap) = removeIP(slots_to_remove, hash, map)
          val newmap1 = newmap - ip_to_remove
          _removeIPs(ips_to_remove.tail, newhash, newmap1)
        } else {
          _removeIPs(ips_to_remove.tail, hash, map)
        }
      }
    }

    if (hosts_to_remove.isEmpty) {
      this
    } else {
      val ips_to_remove = hosts_to_remove.map(_.ip)
      val (newhash, newmap) = _removeIPs(ips_to_remove, hash, ipCountMap)
      if (newmap.isEmpty && newhash.isEmpty) {
        null
      } else {
        val newips = ips.filter(!ips_to_remove.contains(_))
        val newipHostMap = ipHostMap.filter(p => newips.contains(p._1))
        HashRing.initFromParameters(newips, newhash, newipHostMap);
      }
    }
  }

  // add hosts into hashring
  def add(hosts_to_add: Set[Host]): HashRing = {
    // add one ip, return (new hash array, ip count)
    @tailrec
    def addIP(copy_to_add: Int, ip_to_add: String, hash: IndexedSeq[String], map: Map[String, Int]): (IndexedSeq[String], Map[String, Int]) = {
      @tailrec
      def pickSlot(hash: IndexedSeq[String]): Int = {
        val r = HashRing.random.nextInt(HashRing.N)
        if (hash(r).compareTo(ip_to_add) == 0) pickSlot(hash) else r
      }

      if (copy_to_add == 0) {
        (hash, map)
      } else {
        // randomly pick 2 slots, and feed hostToAdd into the one with more slots in hash
        val p = (pickSlot(hash), pickSlot(hash))
        val slot = if (map(hash(p._1)) > map(hash(p._2))) p._1 else p._2
        val newMap = map updated (hash(slot), map(hash(slot)) - 1) // also update count map
        val newHash = hash updated (slot, ip_to_add)
        addIP(copy_to_add - 1, ip_to_add, newHash, newMap)
      }
    }

    // add ips, return hash array
    @tailrec
    def _addIPs(ips_to_add: IndexedSeq[String], hash: IndexedSeq[String], map: Map[String, Int]): IndexedSeq[String] = {
      if (ips_to_add.isEmpty) {
        hash
      } else {
        val num_to_add = HashRing.N / (map.size + 1)
        val (newhash, newmap) = addIP(num_to_add, ips_to_add.head, hash, map)
        val newmap1 = newmap + (ips_to_add.head -> num_to_add)
        _addIPs(ips_to_add.tail, newhash, newmap1)
      }
    }

    if (hosts_to_add.isEmpty) {
      this
    } else {
      val newIpHostMap = this.ipHostMap ++ hosts_to_add.map(host => (host.ip, host.hostname)).toMap
      val ipList_to_add = hosts_to_add.map(_.ip).toIndexedSeq
      val newIps = ips ++ ipList_to_add
      val newhash = _addIPs(ipList_to_add, hash, ipCountMap)

      HashRing.initFromParameters(newIps, newhash, newIpHostMap)
    }
  }

  def size = ips.size

  override def toString(): String = ipCountMap.map(p => (ipHostMap(p._1) -> p._2)).toString

  // equals and hashCode are for unit test
  override def equals(that: Any) : Boolean =
    that.isInstanceOf[HashRing] && (this.hash == that.asInstanceOf[HashRing].hash) && (this.ips == that.asInstanceOf[HashRing].ips);

  override def hashCode = hash.hashCode
}
