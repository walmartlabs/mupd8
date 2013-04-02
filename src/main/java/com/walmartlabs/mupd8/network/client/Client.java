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

    private final Listener listener;
    private ClientBootstrap bootstrap;
    private ClientHandler handler;
    private ConcurrentHashMap<String, Channel> connectors;
    private Executor bossPool;
    private ThreadPoolExecutor workerPool;
    private OneToOneEncoder encoder;
    private Callable<ReplayingDecoder<Decoder.DecodingState>> decoderFactory;
    private static final Logger logger = LoggerFactory.getLogger(Client.class);

    public Client(Listener listener, OneToOneEncoder pencoder, Callable<ReplayingDecoder<Decoder.DecodingState>> pdecoderFactory) {
        this.listener = listener;
        this.encoder = pencoder;
        this.decoderFactory = pdecoderFactory;
    }

    public void init() {

        connectors = new ConcurrentHashMap<String, Channel>();
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

    public boolean connect(String connId, String host, int port) {
        return connect(connId, new InetSocketAddress(host, port));
    }

    public boolean connect(String connId, InetSocketAddress remoteAddr) {

        ChannelFuture future = bootstrap.connect(remoteAddr);

        if (!future.awaitUninterruptibly().isSuccess()) {
            logger.error("CLIENT - Failed to connect to server at " +
                    remoteAddr.getHostName() + ":" + remoteAddr.getPort());
            return false;
        }

        Channel connector = future.getChannel();
        connectors.put(connId, connector);
        return connector.isConnected();
    }

    public boolean isConnected(String connId) {
        if (connectors.containsKey(connId))
            return connectors.get(connId).isConnected();
        else
            return false;
    }

    public void disconnect(String connId) {

        if (connectors.containsKey(connId)) {
            Channel connector = connectors.get(connId);
            if (connector != null) {
                connector.close().awaitUninterruptibly();
            }
            connectors.remove(connId);
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
        if (!connectors.containsKey(connId)) {
            logger.error("CLIENT - connection for " + connId + " doesn't exist!");
            return false;
        }

        Channel connector = connectors.get(connId);
        if (connector.isConnected()) {
            // ChannelBuffer buffer = ChannelBuffers.copiedBuffer(message);
            connector.write(packet);
            return true;
        }
        else {
            logger.error("CLIENT - " + connId + " is not connected!");
            return false;
        }
    }

}
