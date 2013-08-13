package com.walmartlabs.mupd8

import java.util.concurrent.Callable
import grizzled.slf4j.Logging
import org.jboss.netty.handler.codec.oneone.OneToOneEncoder
import org.jboss.netty.handler.codec.replay.ReplayingDecoder
import com.walmartlabs.mupd8.network.server.Server
import com.walmartlabs.mupd8.network.common.Listener
import com.walmartlabs.mupd8.network.client.Client

class MUCluster[T <: MapUpdateClass[T]](app: AppStaticInfo,
                                        val port: Int,
                                        encoder: OneToOneEncoder,
                                        decoderFactory: () => ReplayingDecoder[network.common.Decoder.DecodingState],
                                        onReceipt: T => Unit,
                                        msClient: MessageServerClient = null) extends Logging {
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
    if (host.compareTo(app.self.ip) != 0) client.addEndpoint(host, port)
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

  def send(destip: String, obj: T) {
    if (!client.send(destip, obj)) {
      error("Failed to send event (" + obj + ") to destination " + destip)
      if (msClient != null) {
        error("MUCluster: Report node " + destip + " failure")
        msClient.sendMessage(NodeRemoveMessage(Set(destip)))
      } else {
        error("MUCluster: msclient is null")
      }
    }
  }
}
