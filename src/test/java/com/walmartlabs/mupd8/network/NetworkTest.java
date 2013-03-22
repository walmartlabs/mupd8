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

package com.walmartlabs.mupd8.network;

import junit.framework.TestCase;

import com.walmartlabs.mupd8.network.server.*;
import com.walmartlabs.mupd8.network.client.*;
import com.walmartlabs.mupd8.network.common.*;
import org.jboss.netty.handler.codec.replay.ReplayingDecoder;

import java.util.concurrent.Callable;

public class NetworkTest extends TestCase {

  private Server server1;
  private Server server2;
  private Client client;
  private Packet serverOutput;

  public NetworkTest( String name) {
    super(name);
  }

  public void setUp() {
    Callable<ReplayingDecoder<Decoder.DecodingState>> decoderFactory = new Callable<ReplayingDecoder<Decoder.DecodingState>>() {
      @Override
      public ReplayingDecoder<Decoder.DecodingState> call() throws Exception {
        return new Decoder();
      }
    };

    server1 = new Server(8443, new Listener() {

        @Override
        public boolean messageReceived(Object packet) {
          if (packet instanceof Packet) {
            serverOutput = (Packet)packet;
            return true;
          }
          return false;
        }
      },Encoder.getInstance(), decoderFactory);
    server1.start();

    server2 = new Server(8444, new Listener() {

        @Override
        public boolean messageReceived(Object packet) {
          if (packet instanceof Packet) {
            serverOutput = (Packet)packet;
            return true;
          }
          return false;
        }
      },Encoder.getInstance(), decoderFactory);
    server2.start();

    client = new Client(new Listener() {
        @Override
        public boolean messageReceived(Object packet) {
          // do nothing
          // there is no msg received
          // b/c server doesn't send response
          if (packet instanceof Packet) {
            return true;
          }
          return false;
        }
      },Encoder.getInstance(), decoderFactory);
    client.init();
    // since server only host use key, here use "localhost" and "127.0.0.1" to register 2 client
    client.connect("localhost", 8443);
    client.connect("127.0.0.1", 8444);
  }

  public void testNetwork() throws Exception {
    assertTrue(server1 != null);
    assertTrue(server2 != null);
    assertTrue(client != null);
    Packet first = new Packet(1, 123, "first".getBytes(), "first server".getBytes(), "first stream".getBytes());
    client.send("localhost", first);
    try { Thread.sleep(1000); } catch (Exception e) {}
    System.out.println("output = " + (serverOutput == null));
    assertTrue(serverOutput.toString().equals("{priority=1, performerID=123, key=first, event=first server, stream=first stream}"));
    Packet second = new Packet(2, 456, "second".getBytes(), "second server".getBytes(), "second stream".getBytes());
    client.send("127.0.0.1", second);
    try { Thread.sleep(1000); } catch (Exception e) {}
    assertTrue(serverOutput.toString().equals("{priority=2, performerID=456, key=second, event=second server, stream=second stream}"));
  }

  public void tearDown() {
    client.stop();
    server1.stop();
    server2.stop();
  }
}
