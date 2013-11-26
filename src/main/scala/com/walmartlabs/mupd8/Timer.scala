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

import grizzled.slf4j.Logging
import java.util.TimerTask

// ring: ring in message server, which differs to ring in appruntime
class SendMessage(appRuntime: AppRuntime, ring: HashRing, cmdID: Int, ips: IndexedSeq[String], port: Int, msg: Message, timeout: Int) extends Logging {
  val timer = new java.util.Timer()
  val ip2HostMap = ring.ipHostMap // keep a copy of map in case it is changed
  val lock = new Object()
  var nodesNotResponsed: Set[String] = ips.toSet
  var nodesFailed: Set[String] = Set.empty

  class Sender(val ip: String) extends Runnable {
    val lmsclient = new LocalMessageServerClient(ip, port)

    override def run() = {
      if (!lmsclient.sendMessage(msg)) {
        val t = if (ip2HostMap.contains(ip)) ip2HostMap(ip) else ip
        info("SendNewRing: cmdID " + cmdID + " - send " + msg + " to " + t + " fails")
        lock.synchronized {
          nodesFailed = nodesFailed + ip
        }
      }
      lock.synchronized {
        nodesNotResponsed = nodesNotResponsed - ip
        if (nodesNotResponsed.isEmpty) {
          timer.cancel()
          if (!nodesFailed.isEmpty) {
            // send allDone to message server
            val msClient = new MessageServerClient(appRuntime)
            if (!msClient.sendMessage(AllNodesACKedPrepareMessage(cmdID))) {
              // cannot reach itself, exit
              error("SendMessage: message server is not reachable, exit...")
              System.exit(-1)
            }
          }
        }
      }
    }

    def stop() {
      lmsclient.stop()
    }
  }
  val senders = ips map { new Sender(_) }
  val senderThreads = senders map { new Thread(_) }

  def send() {
    senderThreads.foreach(_.start())

    if (timeout > 0) {
      timer.schedule(new TimerTask() {
        override def run() {
          val nodesToRemove = nodesFailed ++ nodesNotResponsed

          if (!nodesToRemove.isEmpty) {
            info("SendNewRing: failed to send new ring to couple node, send NodeChangeMessage to messageserver")
            val msClient = new MessageServerClient(appRuntime)
            if (!msClient.sendMessage(NodeChangeMessage(Set.empty, nodesToRemove map (ip => Host(ip, ip2HostMap(ip)))))) {
              // cannot reach message server on same node
              error("SendMessage: system is in a very wrong status, exit...")
              System.exit(-1)
            }
          }
        }
      }, timeout)
    }
  }

  def stop() {
    timer.cancel()
    senders.foreach(_.stop())
    senderThreads.foreach(_.interrupt())
  }
}

// ring: ring in message server, which differs to ring in appruntime
class AckCounter(appRuntime: AppRuntime, ring: HashRing, cmdID: Int, ips: Seq[String]) extends Logging {
  val timer = new java.util.Timer()
  var nodesNotAcked = ips.toSet
  val ip2HostMap = ring.ipHostMap
  var isDone = false

  def startCount() {
    timer.schedule(new TimerTask() {
      override def run() {
        if (isDone) {
          info("AckCounter - %d is already done".format(cmdID))
        } else if (!nodesNotAcked.isEmpty) {
          isDone = true
          info("AckCounter: failed to receive Ack from " + nodesNotAcked)
          val msClient = new MessageServerClient(appRuntime)
          if (!msClient.sendMessage(NodeChangeMessage(Set.empty, nodesNotAcked map (ip => Host(ip, ip2HostMap(ip)))))) {
            // cannot reach message server on same node
            error("SendMessage: system is in a very wrong status, exit...")
            System.exit(-1)
          }
        }
      }
    }, 90 * 1000)
  }

  def stop() {
    timer.cancel()
  }

  def count(cmd: Int, ip: String) {
    if (cmd < cmdID) {
      error("AckCounter: cmdID, " + cmd + ", expired; current cmdID = " + cmdID)
    } else if (cmd > cmdID) {
      warn("AckCounter: cmdID, " + cmd + ", hasn't been started")
    } else if (isDone) {
      info("AckCounter: cmdID: %d - this counter is finished".format(cmdID))
    } else {
      this.synchronized {
        nodesNotAcked = nodesNotAcked - ip
        debug("AckCounter: CountPrepareACK - updated nodesNotAcked = " + nodesNotAcked)

        // if all nodes acked cmdID, pin message server
        if (nodesNotAcked.isEmpty) {
          isDone = true
          info("AckedNodeCounter: cmdID - %d all nodes acked".format(cmdID))
          stop()

          scala.actors.Actor.actor {
            // pin message server
            val msClient = new MessageServerClient(appRuntime)
            if (!msClient.sendMessage(AllNodesACKedPrepareMessage(cmdID))) {
              error("AckedNodeCounter: message server is not reachable")
              if (!appRuntime.nextMessageServer.isDefined) {
                error("AckedNodeCounter: couldn't find next message server, exit...");
              }
            }
          }
        }
      }
    }
  }

}

