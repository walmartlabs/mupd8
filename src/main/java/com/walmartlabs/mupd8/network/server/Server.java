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

package com.walmartlabs.mupd8.network.server;

import java.net.InetSocketAddress;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.Executor;
import java.util.concurrent.ThreadPoolExecutor;

import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.group.*;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;

import com.walmartlabs.mupd8.network.common.*;
import org.jboss.netty.handler.codec.oneone.OneToOneEncoder;
import org.jboss.netty.handler.codec.replay.ReplayingDecoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
  * Server to direct messages from clients to workers
  */
public class Server {

    private final int port;
    private final Listener listener;
    private ServerBootstrap bootstrap;
    private ChannelGroup allChannels;
    private Executor bossPool;
    private ThreadPoolExecutor workerPool;
    private OneToOneEncoder encoder;
    private Callable<ReplayingDecoder<Decoder.DecodingState>> decoderFactory;
    private static final Logger logger = LoggerFactory.getLogger(Server.class);

    public Server(int port, Listener listener, OneToOneEncoder pencoder, Callable<ReplayingDecoder<Decoder.DecodingState>> pdecoderFactory) {
        this.port = port;
        this.listener = listener;
        this.encoder = pencoder;
        this.decoderFactory = pdecoderFactory;
    }

    public boolean start() {
        bossPool = Executors.newCachedThreadPool();
        workerPool = (ThreadPoolExecutor)Executors.newCachedThreadPool();
        ChannelFactory factory = new NioServerSocketChannelFactory(bossPool, workerPool);
        allChannels = new DefaultChannelGroup("mupd8-server");

        bootstrap = new ServerBootstrap(factory);
        bootstrap.setOption("reuseAddress", true);
        bootstrap.setOption("child.tcpNoDelay", true);
        bootstrap.setOption("child.keepAlive", true);
        bootstrap.setOption("child.sendBufferSize", 1048576);
        bootstrap.setOption("receiveBufferSize", 1048576);

        // Set up the pipeline factory.
        bootstrap.setPipelineFactory(new ChannelPipelineFactory() {

            @Override
            public ChannelPipeline getPipeline() throws Exception {
                ChannelPipeline pipeline = Channels.pipeline();
                pipeline.addLast("encoder", encoder);
                pipeline.addLast("decoder", decoderFactory.call());
                pipeline.addLast("handler", new ServerHandler(listener));
                return pipeline;
            }
        });
        // Bind and start to accept incoming connections.
        Channel channel = bootstrap.bind(new InetSocketAddress(port));

        if (channel.isBound()) {
        	logger.info("SERVER is bound to *:" + port);
        	allChannels.add(channel);
        	return true;
        } else {
        	logger.error("SERVER failed to bind to *:" + port);
        	bootstrap.releaseExternalResources();
        	return false;
        }
    }

    public void stop() {
        int largestPoolSize = workerPool.getLargestPoolSize();
        logger.info("Largest pool size for server worker pool: " + largestPoolSize);
        allChannels.close().awaitUninterruptibly();
        bootstrap.releaseExternalResources();
        logger.info("SERVER stopped ...");
    }

    public static void main(String[] args) throws Exception {
        int port;

        final Server server;

        if (args.length > 0) {
            port = Integer.parseInt(args[0]);
        }
        else {
            port = 8443;
        }

        server = new Server(port, new Listener() {

            @Override
            public boolean messageReceived(Object packet) {
                if (packet instanceof Packet) {
                    logger.debug("Message received: " + packet.toString());
                    return true;
                }
                return false;
            }
        }, Encoder.getInstance(), new Callable<ReplayingDecoder<Decoder.DecodingState>>() {
            @Override
            public ReplayingDecoder<Decoder.DecodingState> call() throws Exception {
                return new Decoder();
            }
        });
        boolean isStarted = server.start();

        if (!isStarted) {
        	logger.error("Failed to start. Quiting ...");
        	System.exit(1);
        }

        // intercept shutdown signal from VM and shut-down gracefuly.
        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            public void run() {
                server.stop();
            }
        }, "serverShutdownHook"));

    }
}
