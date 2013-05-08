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
    if (getState == Actor.State.Terminated)	this.restart else	this.start
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
case class CountUpdateACK(cmdID: Int, host: String) extends AckedNodeCounterMessage
object AckedNodeCounter extends Actor with Logging {
  private var currentCmdID = -1
  private val nodesNotAcked = scala.collection.mutable.Set[String]()
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
          nodesNotAcked.clear
          nodesNotAcked ++= hosts
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
          nodesNotAcked -= host
          debug("AckedNodeCounter: CountPrepareACK - updated nodesNotAcked = " + nodesNotAcked)

          // if all nodes acked cmdID, pin message server
          if (nodesNotAcked.isEmpty) {
            info("AckedNodeCounter: cmdID, " + cmdID + ", all nodes acked")
            // Stop timer
            TimerActor.stopTimer(cmdID, "Prepare update ring is done")

            // pin message server
            val msClient = new MessageServerClient(mshost, msport)
            msClient.sendMessage(AllNodesACKedPrepareMessage(cmdID))
            currentCmdID = -1
            Actor.exit
          }
        }
        act

      // count acked node
      case CountUpdateACK(cmdID, host) =>
        if (cmdID < currentCmdID)
          error("AckedNodeCounter: cmdID, " + cmdID + ", expired 1; current cmdID = " + currentCmdID)
        else if (cmdID > currentCmdID)
          warn("AckedNodeCounter: cmdID, " + cmdID + ", hasn't been started 1")
        else {
          // clear host
          nodesNotAcked -= host
          debug("AckedNodeCounter: CountUpdateACK - updated nodesNotAcked = " + nodesNotAcked)

          // TODO: if all nodes acked cmdID, pin message server
          if (nodesNotAcked.isEmpty) {
            currentCmdID = -1
            Actor.exit
          }
        }
        act

      case "EXIT" =>
        info("Exit AckedNodeCounter")
    }
  }
}
