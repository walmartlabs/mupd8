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

abstract class Message extends Serializable
abstract class MessageWOACK extends Message // message needs NO ACK
abstract class MessageWACK extends Message // message needs ACK
// To message server
case class NodeRemoveMessage(ip: String) extends MessageWACK  // node: Host(ip, hostname)
case class NodeJoinMessage(node: Host) extends MessageWACK      // node: Host(ip, hostname)
case class ACKPrepareAddHostMessage(cmdID: Int, hostToAdd: String) extends MessageWOACK         // hostToAdd: ip
case class ACKPrepareRemoveHostMessage(cmdID: Int, hostToRemove: String) extends MessageWOACK   // hostToRemove: ip
case class AllNodesACKedPrepareMessage(cmdID: Int) extends MessageWOACK
case class ACKTIMEOUTMessage(cmdID: Int) extends MessageWACK
// for ip check
case class IPCHECKDONE() extends MessageWOACK

// To local message server
// cmdID
// addedHost: Host object of node to be added
// hashInNewRing: ip address array used as hashtable in new hash ring
// iPsInNewRing: all ip addresses in new ring
// iPHostMap: ip address to host name map
case class PrepareAddHostMessage(cmdID: Int, addedHost: Host, hashInNewRing: IndexedSeq[String], iPsInNewRing: IndexedSeq[String], iP2HostMap: Map[String, String]) extends MessageWOACK {
  override def toString() = "PrepareAddHostMessage(" + cmdID + ", " + addedHost.hostname + ")"
}
// cmdID
// removedIP: ip of node to be removed
// hashInNewRing: ip address array used as hashtable in new hash ring
// iPsInNewRing: all ip addresses in new ring
// iPHostMap: ip address to host name map
case class PrepareRemoveHostMessage(cmdID: Int, removedIP: String, hashInNewRing: IndexedSeq[String], iPsInNewRing: IndexedSeq[String], iP2HostMap: Map[String, String]) extends MessageWOACK {
  override def toString() = "PrepareRemoveHostMessage(" + cmdID + ", " + removedIP + ")"
}
case class UpdateRing(cmdID: Int) extends MessageWACK

// To MessageServerClient
case class ACKNodeJoin(node: String) extends MessageWACK   // node: ip
case class ACKNodeRemove(node: String) extends MessageWACK // node: ip

//
// Start source readers
// 2 cases:
// 1. Performer node according to config file asks message server if  it can start certain source
// 2. Message server dictate to performer node to start certain source
//
// To message server
case class AskPermitToStartSourceMessage(name: String, host: Host) extends MessageWACK
case class ACKMessage() extends MessageWOACK

// To performer node
case class StartSourceMessage(name: String) extends MessageWACK
