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

package com.walmartlabs.mupd8.network.common;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandler;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.handler.codec.oneone.OneToOneEncoder;

/**
  * OneToOneEncoder implementation that converts a Packet instance into a ChannelBuffer.
  *
  * Since the encoder is stateless, a single instance can be shared among all pipelines, hence the @Sharable annotation
  * and the singleton instantiation.
  */

@ChannelHandler.Sharable
public class Encoder extends OneToOneEncoder {

    private Encoder() {
    }

    public static Encoder getInstance() {
        return InstanceHoder.INSTANCE;
    }

    public static ChannelBuffer encodeMessage(Packet packet) throws IllegalArgumentException {

        if (packet.getKey() == null || packet.getKey().length == 0) {
            throw new IllegalArgumentException("Packet key cannot be null or empty");
        }

        if (packet.getEvent() == null || packet.getEvent().length == 0) {
            throw new IllegalArgumentException("Packet event cannot be null or empty");
        }

        int size = 4 + 4 +                           // priority(4bytes) + performerID(4bytes)
        		   4 + packet.getKey().length +      // key_length(4bytes) + key(nbytes)
        		   4 + packet.getEvent().length +    // event_length(4bytes) + event(nbytes)
        		   4 + packet.getStream().length;    // stream_length(4bytes) + stream(nbytes)

        ChannelBuffer buffer = ChannelBuffers.buffer(size);
        buffer.writeInt(packet.getPriority());
        buffer.writeInt(packet.getPerformerID());
        buffer.writeInt(packet.getKey().length);
        buffer.writeBytes(packet.getKey());
        buffer.writeInt(packet.getEvent().length);
        buffer.writeBytes(packet.getEvent());
        buffer.writeInt(packet.getStream().length);
        buffer.writeBytes(packet.getStream());

        return buffer;
    }

    @Override
    protected Object encode(ChannelHandlerContext channelHandlerContext, Channel channel, Object msg) throws Exception {
        if (msg instanceof Packet) {
            return encodeMessage((Packet) msg);
        } else {
            return msg;
        }
    }

    private static final class InstanceHoder {
        private static final Encoder INSTANCE = new Encoder();
    }
}
