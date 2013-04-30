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
// To message server
case class NodeRemoveMessage(val node: String) extends Message
case class NodeJoinMessage(val node: String) extends Message

// To local message server
case class AddHostMessage(val cmdID: Int, val addedHost: String, val hashInNewRing: IndexedSeq[String], val hostsInNewRing: IndexedSeq[String]) extends Message {
  override def toString() = "AddHostMessage(" + cmdID + ", " + addedHost + ", " + hostsInNewRing + ")"
}
case class RemoveHostMessage(val cmdID: Int, val removedHost: String, val hashInNewRing: IndexedSeq[String], val hostsInNewRing: IndexedSeq[String]) extends Message {
  override def toString() = "RemoveHostMessage(" + cmdID + ", " + removedHost + ", " + hostsInNewRing + ")"
}

case class AckOfNewRing(val commandId: Int) extends Message
case class AckOfNodeJoin(val node: String) extends Message
case class AckOfNodeRemove(val node: String) extends Message
