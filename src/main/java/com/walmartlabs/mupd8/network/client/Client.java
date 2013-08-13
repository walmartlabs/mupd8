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

package com.walmartlabs.mupd8.network.client;

import java.net.InetSocketAddress;
import java.util.concurrent.*;

import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;

import com.walmartlabs.mupd8.network.common.*;
import org.jboss.netty.handler.codec.oneone.OneToOneEncoder;
import org.jboss.netty.handler.codec.replay.ReplayingDecoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Client {

  private class Endpoint {
    public String _host;
    public int    _port;

    public Endpoint(String host, int port) {
      _host = host;
      _port = port;
    }
    
    @Override
    public String toString() {
      return "(" + _host + ", " + _port + ")";
    }
  }

  private final Listener listener;
  private ClientBootstrap bootstrap;
  private ClientHandler handler;
  // Store connections to all hosts in cluster
  private ConcurrentHashMap<String, Channel> connectors;
  // Store endpoints for all hosts in cluster
  private ConcurrentHashMap<String, Endpoint> endpoints;
  private Executor bossPool;
  private ThreadPoolExecutor workerPool;
  private OneToOneEncoder encoder;
  private Callable<ReplayingDecoder<Decoder.DecodingState>> decoderFactory;
  private static final Logger logger = LoggerFactory.getLogger(Client.class);

  public Client(Listener listener, OneToOneEncoder pencoder, Callable<ReplayingDecoder<Decoder.DecodingState>> pdecoderFactory) {
    this.listener = listener;
    this.encoder = pencoder;
    this.decoderFactory = pdecoderFactory;
    connectors = new ConcurrentHashMap<String, Channel>();
    endpoints = new ConcurrentHashMap<String, Endpoint>();
  }

  public void init() {
    // Standard netty bootstrapping stuff.
    bossPool = Executors.newCachedThreadPool();
    workerPool = (ThreadPoolExecutor)Executors.newCachedThreadPool();
    ChannelFactory factory =
      new NioClientSocketChannelFactory(bossPool, workerPool);

    handler = new ClientHandler(listener);
    bootstrap = new ClientBootstrap(factory);

    bootstrap.setOption("reuseAddress", true);
    bootstrap.setOption("tcpNoDelay", true);
    bootstrap.setOption("keepAlive", true);
    bootstrap.setOption("sendBufferSize", 1048576);
    bootstrap.setOption("receiveBufferSize", 1048576);
    bootstrap.setPipelineFactory(new ChannelPipelineFactory() {

        @Override
        public ChannelPipeline getPipeline() throws Exception {
          ChannelPipeline pipeline = Channels.pipeline();
          pipeline.addLast("encoder", encoder);
          pipeline.addLast("decoder", decoderFactory.call());
          pipeline.addLast("handler", handler);
          return pipeline;
        }
      });
  }

  public void addEndpoint(String host, int port) {
    endpoints.put(host, this.new Endpoint(host, port));
  }

  public void removeEndpoint(String host) {
    if (endpoints.containsKey(host)) {
      endpoints.remove(host);
    }
    disconnect(host);
  }

  private boolean connect(String host, int port) {
    InetSocketAddress remoteAddr = new InetSocketAddress(host, port);

    ChannelFuture future = null;
    // retry 3 times for now
    int i = 0;
    for (; i < 3; i++) {
      future= bootstrap.connect(remoteAddr);
      if (!future.awaitUninterruptibly().isSuccess()) {
        logger.error("CLIENT - Failed to connect to server at " +
                    remoteAddr.getHostName() + ":" + remoteAddr.getPort());
        try {
          Thread.sleep(500);
        } catch (InterruptedException e) {
        }
      } else break;
    }
    if (i >= 3) return false;
    logger.info("CLIENT - Connected to " + remoteAddr.getHostName() + ":" + remoteAddr.getPort());

    Channel connector = future.getChannel();
    connectors.put(host, connector);
    return connector.isConnected();
  }

  public boolean isConnected(String connId) {
    if (connectors.containsKey(connId))
      return connectors.get(connId).isConnected();
    else
      return false;
  }

  private void disconnect(String host) {
    if (connectors.containsKey(host)) {
      Channel connector = connectors.get(host);
      if (connector != null) {
        connector.close().awaitUninterruptibly();
      }
      connectors.remove(host);
    }
  }

  // close all the connections and shut down the client
  public void stop() {
    int largestPoolSize = workerPool.getLargestPoolSize();
    logger.info("Largest pool size for client worker pool: " + largestPoolSize);
    for (Channel connector : connectors.values()) {
      if (connector != null)
        connector.close().awaitUninterruptibly();
    }

    connectors.clear();
    this.bootstrap.releaseExternalResources();
    logger.info("CLIENT stopped...");
  }

  public boolean send(String connId, Object packet) {
    if (!endpoints.containsKey(connId)) {
      logger.warn("CLIENT - endpoint of " + connId + " doesn't exist.");
      return false;
    }

    // Make connect only when it is used.
    if (!connectors.containsKey(connId)) {
      logger.warn("CLIENT - connection to (" + connId + ", " + endpoints.get(connId) + ") doesn't exist; going to make connection.");
      if (!connect(endpoints.get(connId)._host, endpoints.get(connId)._port)) {
        logger.error("CLIENT - connecting to " + connId + " failed.");
        return false;
      }
    }

    Channel connector = connectors.get(connId);
    if (connector.isConnected()) {
      connector.write(packet);
      return true;
    } else {
      logger.error("CLIENT - " + connId + " is not connected!");
      if (!connect(endpoints.get(connId)._host, endpoints.get(connId)._port)) {
        logger.error("CLIENT - reconnecting to " + connId + " failed.");
        return false;
      }
      connector = connectors.get(connId);
      if (connector.isConnected()) {
        connector.write(packet);
        return true;
      } else {
        logger.error("CLIENT - " + connId + " still is not connected!");
        return false;
      }
    }
  }

}
