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

import com.walmartlabs.mupd8.GT._
import com.walmartlabs.mupd8.Misc._
import com.walmartlabs.mupd8.Misc._
import com.walmartlabs.mupd8.Mupd8Type._
import grizzled.slf4j.Logging
import org.jboss.netty.channel.ChannelHandlerContext
import org.jboss.netty.channel.Channel
import org.jboss.netty.buffer.ChannelBuffer
import org.jboss.netty.buffer.ChannelBuffers
import org.jboss.netty.handler.codec.oneone.OneToOneEncoder

trait MapUpdateClass[T] extends OneToOneEncoder with Runnable with Comparable[T] with java.io.Serializable {
  def getKey: PerformerPacketKey
}

object PerformerPacket {
  @inline def getKey(pid: Int, key: Key): PerformerPacketKey = new PerformerPacketKey(pid, key)
}

case class PerformerPacketKey(val pid: Int, val slateKey: Key)

// TODO: create PerformerPacketKey class
case class PerformerPacket(pri: Priority,
                           pid: Int,
                           slateKey: Key,
                           event: Event,
                           stream: String, // This field can be replaced by source performer ID
                           appRun: AppRuntime) extends MapUpdateClass[PerformerPacket] with Logging {
  override def getKey = new PerformerPacketKey(pid, slateKey)

  override def compareTo(other: PerformerPacket) = pri.compareTo(other.pri)

  override def toString = "{" + pri + "," + pid + "," + str(slateKey.value) + "," + str(event) + "," + stream + "}"

  // Treat this as a static method, do not touch "this", use msg
  override protected def encode(channelHandlerContext: ChannelHandlerContext, channel: Channel, msg: AnyRef): AnyRef = {
    if (msg.isInstanceOf[PerformerPacket]) {
      val packet = msg.asInstanceOf[PerformerPacket]
      val size: Int = 4 + 4 + 4 + packet.slateKey.value.length + 4 + packet.event.length + 4 + packet.stream.length
      val buffer: ChannelBuffer = ChannelBuffers.buffer(size)
      buffer.writeInt(packet.pri)
      buffer.writeInt(packet.pid)
      buffer.writeInt(packet.slateKey.value.length)
      buffer.writeBytes(packet.slateKey.value)
      buffer.writeInt(packet.event.length)
      buffer.writeBytes(packet.event)
      buffer.writeInt(packet.stream.length)
      buffer.writeBytes(packet.stream.getBytes) // TODO: Stream name encoding assumption
      buffer
    } else
      msg
  }

  def run() {
    def execute(x: => Unit) {
      val result = excToOptionWithLog(x)
      if (result == None) error("Bad exception Bro")
    }

    def executeUpdate(tls: TLS, slate: SlateObject) {
      if (appRun.ring != null) { // ring could be null at init
        if (appRun.candidateRing != null
          && appRun.ring(getKey) == appRun.self.ip
          && appRun.candidateRing(getKey) != appRun.self.ip) {
          // if in ring change process and dest of this slate changes by candidate ring
          //appRun.pool.putLocal(getKey, this)
          appRun.eventBufferForRingChange.offer(this)
        } else if (appRun.candidateRing == null && appRun.ring(getKey) != appRun.self.ip) {
          // if not in ring change process and dest of this slate is not this node
          val destip = appRun.ring(getKey)
          appRun.pool.cluster.send(Host(destip, appRun.ring.ipHostMap(destip)), this)
        } else {
          execute(appRun.getUpdater(pid).update(tls, stream, slateKey.value, event, slate))
        }
      }
    }

    val performer = appRun.appStatic.performers(pid)
    val name = performer.name

    val cache = appRun.getTLS(pid, slateKey).slateCache
    val optSlate = if (performer.mtype == Updater) Some {
      val s = cache.getSlate((name, slateKey))
      s map {
        p => trace("Succeeded for " + name + "," + slateKey + " " + p)
      } getOrElse {
        trace("Failed fetch for " + name + "," + slateKey)
        // Re-introduce self after issuing a read
        cache.waitForSlate((name,slateKey),_ => appRun.pool.put(this.getKey, this), appRun.getUpdater(pid, slateKey), appRun.getSlateBuilder(pid))
      }
      s
    } else
      None

    if (optSlate != Some(None)) {
      val slate = optSlate.flatMap(p => p)
      val tls = appRun.getTLS
      tls.perfPacket = this
      tls.startTime = java.lang.System.nanoTime()
      (performer.mtype, tls.unifiedUpdaters(pid)) match {
        case (Updater, true) => error("UnifiedUpdater is not supported anymore")
        // execute(appRun.getUnifiedUpdater(pid).update(tls, stream, slateKey.value, Array(event), Array(slate.get)))
        case (Updater, false) => executeUpdate(tls, slate.get)
        case (Mapper, false) => execute(appRun.getMapper(pid).map(tls, stream, slateKey.value, event))
        case (x, y) => error("PerformerPacket: Wrong pair - " + (x, y))
      }
      tls.perfPacket = null
      tls.startTime = 0
      trace("Executed " + performer.mtype.toString + " " + name)
    }
  }
}
