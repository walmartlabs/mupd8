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

import scala.actors._
import scala.actors.Actor._
import grizzled.slf4j.Logging

abstract class TimerActorMessage
case class Stop(reason: AnyRef) extends TimerActorMessage
object TimerActor extends Actor with Logging {
  private var _timeout = -1L
  private var _cmdID = -1
  private var func: () => Unit = null

  def startTimer(cmdID: Int, timeout: Long, f: () => Unit) {
    _timeout = timeout
    _cmdID = cmdID
    func = f
    info("Timer starts: cmdID = " + _cmdID + ", timeout = " + _timeout)
    if (this.getState == Actor.State.Terminated) {
      this.restart
      info("Timer restarted")
    } else {
	    this.start
      info("Timer started")
    }
  }

  def stopTimer(cmdID: Int, reason: AnyRef) {
    info("Timer exit: cmdID = " + _cmdID + ", reason = " + reason)
    this ! Stop(reason)
  }

  override def act {
    self.reactWithin(_timeout) {
      case TIMEOUT => func
      case Stop(reason) =>
        info("TimerActor stop: " + reason)
        this.exit
    }
  }
}

// AckedNodeCounter counts how many nodes already ACK Prepare[Add|Remove]HostMessage
// and Update[Add|Remove]HostMessage. And if left nodes to ACK set is empty, pin message server
abstract class AckedNodeCounterMessage
case class StartCounter(cmdID: Int, hosts: IndexedSeq[String], _mshost: String, _msport: Int) extends AckedNodeCounterMessage
case class CountPrepareACK(cmdID: Int, host: String) extends AckedNodeCounterMessage
object AckedNodeCounter extends Actor with Logging {
  private var currentCmdID = -1
  var nodesNotAcked = scala.collection.immutable.Set[String]()
  private var mshost: String = null
  private var msport: Int = -1

  def act {
    react {
      // reset/start counter
      case StartCounter(cmdID, hosts, _mshost, _msport) =>
        if (cmdID < currentCmdID)
          error("AckedNodeCounter: cmdID, " + cmdID + ", expired; current cmdID = " + currentCmdID)
        else {
          info("AckedNodeCounter: Start counter for cmdID, " + cmdID)
          currentCmdID = cmdID
          nodesNotAcked = null
          nodesNotAcked = hosts.toSet
          mshost = _mshost
          msport = _msport
        }
        act

      // count acked node
      case CountPrepareACK(cmdID, host) =>
        if (cmdID < currentCmdID)
          error("AckedNodeCounter: cmdID, " + cmdID + ", expired; current cmdID = " + currentCmdID)
        else if (cmdID > currentCmdID)
          warn("AckedNodeCounter: cmdID, " + cmdID + ", hasn't been started")
        else {
          // clear host
          nodesNotAcked = nodesNotAcked - host
          info("AckedNodeCounter: CountPrepareACK - updated nodesNotAcked = " + nodesNotAcked)

          // if all nodes acked cmdID, pin message server
          if (nodesNotAcked.isEmpty) {
            info("AckedNodeCounter: cmdID, " + cmdID + ", all nodes acked")
            // Stop timer
            TimerActor.stopTimer(cmdID, "Prepare update ring is done")

            // pin message server
            val msClient = new MessageServerClient(mshost, msport)
            msClient.sendMessage(AllNodesACKedPrepareMessage(cmdID))
            currentCmdID = -1
          }
        }
        act

      case "EXIT" =>
        info("Exit AckedNodeCounter")
    }
  }
}
