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
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.handler.codec.replay.ReplayingDecoder;

public class Decoder extends ReplayingDecoder<Decoder.DecodingState> {

    private Packet packet;

    public Decoder() {
        this.reset();
    }

    private void reset() {
        checkpoint(DecodingState.PRIORITY);
        packet = new Packet();
    }

    @Override
    protected Object decode(ChannelHandlerContext ctx, Channel channel,
                            ChannelBuffer buffer, DecodingState state)
            throws Exception {

        // switch fall through
        switch(state) {
            case PRIORITY:
                packet.setPriority(buffer.readInt());
                checkpoint(DecodingState.PERFORMERID);
            case PERFORMERID:
                packet.setPerformerID(buffer.readInt());
                checkpoint(DecodingState.KEY_LENGTH);
            case KEY_LENGTH:
                int keyLen = buffer.readInt();
                if (keyLen <= 0) {
                    throw new Exception("Invalid content size");
                }
                byte[] key = new byte[keyLen];
                packet.setKey(key);
                checkpoint(DecodingState.KEY);
            case KEY:
                buffer.readBytes(packet.getKey(), 0,
                                 packet.getKey().length);
                checkpoint(DecodingState.EVENT_LENGTH);
            case EVENT_LENGTH:
                int eventLen = buffer.readInt();
                if (eventLen <= 0) {
                    throw new Exception("Invalid content size");
                }
                byte[] event = new byte[eventLen];
                packet.setEvent(event);
                checkpoint(DecodingState.EVENT);
            case EVENT:
                buffer.readBytes(packet.getEvent(), 0,
                                 packet.getEvent().length);
                checkpoint(DecodingState.STREAM_LENGTH);
            case STREAM_LENGTH:
                int streamLen = buffer.readInt();
                byte[] stream = new byte[streamLen];
                packet.setStream(stream);
                checkpoint(DecodingState.STREAM);
            case STREAM:
                buffer.readBytes(packet.getStream(), 0,
                                 packet.getStream().length);
                // only exit point of the method
                try {
                    return packet;
                } finally {
                    reset();
                }
            default:
                throw new Exception("Unknown decoding state: " + state);
        }

    }

    public enum DecodingState {
        PRIORITY,
        PERFORMERID,
        KEY_LENGTH,
        KEY,
        EVENT_LENGTH,
        EVENT,
        STREAM_LENGTH,
        STREAM
    }
}
