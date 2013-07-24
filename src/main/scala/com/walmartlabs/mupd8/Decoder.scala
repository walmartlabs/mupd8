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

import org.jboss.netty.handler.codec.replay.ReplayingDecoder
import com.walmartlabs.mupd8.network.common.Decoder.DecodingState._
import com.walmartlabs.mupd8.network.common.Decoder.DecodingState
import com.walmartlabs.mupd8.network.common._
import com.walmartlabs.mupd8.GT._
import com.walmartlabs.mupd8.Misc._
import org.jboss.netty.channel.ChannelHandlerContext
import org.jboss.netty.channel.Channel
import org.jboss.netty.buffer.ChannelBuffer

class Decoder(val appRun: AppRuntime) extends ReplayingDecoder[DecodingState](PRIORITY) {
  var pri: Priority = -1
  var pid: Int = -1
  var key: Key = Key(new Array[Byte](0))
  var event: Event = Array()
  var stream: Array[Byte] = Array()

  reset()
  private def reset() {
    checkpoint(PRIORITY)
    pri = -1
    pid = -1
    key = Key(new Array[Byte](0))
    event = Array()
    stream = Array()
  }

  protected def decode(ctx: ChannelHandlerContext, channel: Channel, buffer: ChannelBuffer, stateParam: DecodingState): AnyRef = {
    var p: PerformerPacket = null
    var state = stateParam

    do {
      //      (state.## : @scala.annotation.switch) match {
      state match {
        case PRIORITY =>
          pri = buffer.readInt
          checkpoint(PERFORMERID)
        case PERFORMERID =>
          pid = buffer.readInt
          checkpoint(KEY_LENGTH)
        case KEY_LENGTH =>
          val keyLen = buffer.readInt
          if (keyLen < 0) {
            throw new Exception("Invalid key size")
          }
          key = Key(new Array[Byte](keyLen))
          checkpoint(KEY)
        case KEY =>
          buffer.readBytes(key.value, 0, key.value.length)
          checkpoint(EVENT_LENGTH)
        case EVENT_LENGTH =>
          val eventLen = buffer.readInt
          if (eventLen < 0) {
            throw new Exception("Invalid event size")
          }
          event = new Array[Byte](eventLen)
          checkpoint(EVENT)
        case EVENT =>
          buffer.readBytes(event, 0, event.length)
          checkpoint(STREAM_LENGTH)
        case STREAM_LENGTH =>
          val streamLen = buffer.readInt
          if (streamLen < 0) {
            throw new Exception("Invalid stream size")
          }
          stream = new Array[Byte](streamLen)
          checkpoint(STREAM)
        case STREAM =>
          buffer.readBytes(stream, 0, stream.length)
          p = PerformerPacket(pri, pid, key, event, str(stream), appRun)
          reset()
        case _ =>
          throw new Exception("Unknown decoding state: " + state)
      }
      state = getState
    } while (state != PRIORITY)
    //    try { return p } finally { reset() }
    p
  }
}
