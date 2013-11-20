package com.walmartlabs.mupd8

import java.util.concurrent.Callable
import grizzled.slf4j.Logging
import org.jboss.netty.handler.codec.oneone.OneToOneEncoder
import org.jboss.netty.handler.codec.replay.ReplayingDecoder
import com.walmartlabs.mupd8.network.server.Server
import com.walmartlabs.mupd8.network.common.Listener
import com.walmartlabs.mupd8.network.client.Client

class MUCluster[T <: MapUpdateClass[T]](self: Host,
                                        val port: Int,
                                        encoder: OneToOneEncoder,
                                        decoderFactory: () => ReplayingDecoder[network.common.Decoder.DecodingState],
                                        onReceipt: T => Unit,
                                        appRun: AppRuntime) extends Logging {
  private val callableFactory = new Callable[ReplayingDecoder[network.common.Decoder.DecodingState]] {
    override def call() = decoderFactory()
  }

  val server = new Server(port, new Listener() {
    override def messageReceived(packet: AnyRef): Boolean = {
      val destObj = packet.asInstanceOf[T]
      trace("Server receives: " + destObj)
      onReceipt(destObj)
      true
    }
  }, encoder, callableFactory)
  server.start()
  info("MUCluster: MUCluster server at " + port + " started")

  val client = new Client(new Listener() {
    override def messageReceived(packet: AnyRef): Boolean = {
      error("Client should not receive messages")
      true
    }
  }, encoder, callableFactory)
  client.init()

  // Add host to connection map
  def addHost(host: String) {
    if (host.compareTo(self.ip) != 0) client.addEndpoint(host, port)
  }

  def addHosts(hosts: Set[String]) {
    hosts.foreach(addHost(_))
  }

  // Remove host from connection map
  def removeHost(host: String) {
    client.removeEndpoint(host)
  }

  def removeHosts(hosts: Set[String]) {
    hosts.foreach(removeHost(_))
  }

  def send(dest: Host, obj: T) {
    def _send(retryCount: Int, destip: String, obj: T): Boolean = {
      if (retryCount == 0) {
        false
      } else {
        if (!client.send(destip, obj)) {
          warn("Failed to send event (" + obj + ") to destination " + destip + " at retry : " + retryCount)
          Thread.sleep(10000)
          _send(retryCount - 1, destip, obj)
        } else {
          true
        }
      }
    }

    if (!_send(6, dest.ip, obj)) {
      error("Failed to send event (" + obj + ") to destination " + dest)
      if (appRun.msClient != null) {
        error("MUCluster: Report node " + dest + " failure")
        if (!appRun.msClient.sendMessage(NodeChangeMessage(Set.empty, Set(dest)))) {
          error("MUCluster: message server is not reachable")
          if (!appRun.nextMessageServer().isDefined) {
            error("MUCluster: couldn't find next messagese, exit...")
            System.exit(-1);
          }
        }
      } else {
        error("MUCluster: msclient is null")
      }
    }
  }
}
