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
case class NodeRemoveMessage(node: String) extends MessageWACK
case class NodeJoinMessage(node: String) extends MessageWACK
case class ACKPrepareAddHostMessage(cmdID: Int, hostToAdd: String) extends MessageWOACK
case class ACKPrepareRemoveHostMessage(cmdID: Int, hostToRemove: String) extends MessageWOACK
case class AllNodesACKedPrepareMessage(cmdID: Int) extends MessageWOACK
case class ACKTIMEOUTMessage(cmdID: Int) extends MessageWACK

// To local message server
case class PrepareAddHostMessage(cmdID: Int, addedHost: String, hashInNewRing: IndexedSeq[String], hostsInNewRing: IndexedSeq[String]) extends MessageWOACK {
  override def toString() = "AddHostMessage(" + cmdID + ", " + addedHost + ", " + hostsInNewRing + ")"
}
case class PrepareRemoveHostMessage(cmdID: Int, removedHost: String, hashInNewRing: IndexedSeq[String], hostsInNewRing: IndexedSeq[String]) extends MessageWOACK {
  override def toString() = "RemoveHostMessage(" + cmdID + ", " + removedHost + ", " + hostsInNewRing + ")"
}
case class UpdateRing(cmdID: Int) extends MessageWACK

// To MessageServerClient
case class ACKNodeJoin(node: String) extends MessageWACK
case class ACKNodeRemove(node: String) extends MessageWACK
