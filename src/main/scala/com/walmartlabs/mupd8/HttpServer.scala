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

import java.util.concurrent.Executors
import java.net.InetSocketAddress
import org.jboss.netty.bootstrap.ServerBootstrap
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory
import org.jboss.netty.channel.ChannelPipelineFactory
import org.jboss.netty.channel.ChannelFuture
import org.jboss.netty.channel.ChannelFutureListener
import org.jboss.netty.channel.group.DefaultChannelGroup
import org.jboss.netty.channel.ChannelPipeline
import org.jboss.netty.channel.Channels
import org.jboss.netty.channel.SimpleChannelUpstreamHandler
import org.jboss.netty.channel.ChannelHandlerContext
import org.jboss.netty.channel.ExceptionEvent
import org.jboss.netty.channel.MessageEvent
import org.jboss.netty.handler.codec.http._
import org.jboss.netty.handler.codec.http.HttpHeaders._
import org.jboss.netty.handler.codec.http.HttpMethod._
import org.jboss.netty.handler.codec.http.HttpResponseStatus._
import org.jboss.netty.handler.codec.http.HttpVersion._
import org.jboss.netty.buffer.ChannelBuffers
import org.jboss.netty.util.CharsetUtil
import grizzled.slf4j.Logging

class HttpServer(port : Int, maxWorkerCount : Int, mapper : String => Option[Array[Byte]]) extends Logging {

  var allChannels : DefaultChannelGroup = null
  var bootstrap : ServerBootstrap = null

  def start : Boolean = {
    val bossPool   = Executors.newCachedThreadPool
    val workerPool = Executors.newCachedThreadPool
    val factory = new NioServerSocketChannelFactory(bossPool, workerPool, maxWorkerCount)
    allChannels = new DefaultChannelGroup("mupd8-slateserver")
    bootstrap = new ServerBootstrap(factory)
    bootstrap.setOption("reuseAddress", true)
    bootstrap.setOption("child.tcpNoDelay", true)
    bootstrap.setOption("child.keepAlive", true)
    bootstrap.setOption("receiveBufferSize", 4194304)
    // Set up the pipeline factory.
    bootstrap.setPipelineFactory(new ChannelPipelineFactory() {
    	override def getPipeline : ChannelPipeline = {
    		val pipeline : ChannelPipeline = Channels.pipeline()
    		pipeline.addLast("decoder", new HttpRequestDecoder(12288, 12288, 12288))
    		pipeline.addLast("encoder", new HttpResponseEncoder())
    		pipeline.addLast("handler", new HttpServerHandler(mapper))
    		pipeline;
    	}
    });

    // Bind and start to accept incoming connections.
    val channel = bootstrap.bind(new InetSocketAddress(port))

    if (channel.isBound()) {
      allChannels.add(channel)
      true
    }
    else {
      bootstrap.releaseExternalResources
      false
    }
  }

  def stop : Unit = {
    allChannels.close().awaitUninterruptibly
    bootstrap.releaseExternalResources
  }
}

class HttpServerHandler(mapper : String => Option[Array[Byte]]) extends SimpleChannelUpstreamHandler with Logging {

  override def exceptionCaught(ctx : ChannelHandlerContext, e : ExceptionEvent) : Unit = {
    // Close the connection when an exception is raised.
    error("HttpServerHandler exception.", e.getCause())
    e.getChannel().close
  }

  override def messageReceived(ctx : ChannelHandlerContext, e : MessageEvent) : Unit = {
    val msg : AnyRef = e.getMessage;
    if (msg.isInstanceOf[HttpRequest]) {
      trace("HttpServer: received " + msg.asInstanceOf[HttpRequest])
      handleHttpRequest(ctx, msg.asInstanceOf[HttpRequest]);
    } else {
      super.messageReceived(ctx, e);
    }
  }

  private def handleHttpRequest(ctx : ChannelHandlerContext, req : HttpRequest) : Unit = {
    if (req.getMethod() == POST) {
      val res = new DefaultHttpResponse(HTTP_1_1, OK)
      res.setHeader("Content-Type", "application/json")
      val content = ChannelBuffers.copiedBuffer("{}", CharsetUtil.UTF_8)
      setContentLength(res, content.readableBytes)
      res.setContent(content);
      sendHttpResponse(ctx, req, res)
    } else if (req.getMethod == GET) {
      val uri : String = req.getUri
      debug("GET " + uri)
      mapper(uri) map { response =>
        // TODO Set appropriate Content-Type (implement MIME magic).
        val contentType = if (response.startsWith("{")) "application/json" else "application/octet-stream"
        val res = new DefaultHttpResponse(HTTP_1_1, OK)
        res.setHeader("Content-Type", contentType)
        val content = ChannelBuffers.wrappedBuffer(response)
        setContentLength(res, content.readableBytes)
        res.setContent(content)
        sendHttpResponse(ctx, req, res)
      } getOrElse {
        sendHttpResponse(ctx, req, new DefaultHttpResponse(HTTP_1_1, NOT_FOUND))
      }
    }
    else {
      sendHttpResponse(ctx, req, new DefaultHttpResponse(HTTP_1_1, BAD_REQUEST))
    }
  }

  private def sendHttpResponse(ctx : ChannelHandlerContext, req : HttpRequest, res : HttpResponse) : Unit = {
    // Generate an error page if response status code is not OK (200).
    if (res.getStatus().getCode() != 200) {
      res.setContent(ChannelBuffers.copiedBuffer(res.getStatus().toString(), CharsetUtil.UTF_8))
      setContentLength(res, res.getContent().readableBytes())
    }
    // Send the response and close the connection if necessary.
    val f : ChannelFuture = ctx.getChannel().write(res)
    if (!isKeepAlive(req) || res.getStatus().getCode() != 200) {
      f.addListener(ChannelFutureListener.CLOSE)
    }
  }
}
