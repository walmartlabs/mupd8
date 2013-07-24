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

  // hosts can be updated at runtime
  def hosts = app.systemHosts

  val server = new Server(port, new Listener() {
    override def messageReceived(packet: AnyRef): Boolean = {
      val destObj = packet.asInstanceOf[T]
      trace("Server receives: " + destObj)
      onReceipt(destObj)
      true
    }
  }, encoder, callableFactory)

  val client = new Client(new Listener() {
    override def messageReceived(packet: AnyRef): Boolean = {
      error("Client should not receive messages")
      assert(false)
      true
    }
  }, encoder, callableFactory)
  client.init()

  def init() {
    server.start()
    hosts.filterKeys(_.compareTo(app.self.ip) != 0).foreach (host => client.addEndpoint(host._1, port))
  }

  // Add host to connection map
  def addHost(host: String): Unit = if (host.compareTo(app.self.ip) != 0) client.addEndpoint(host, port)

  // Remove host from connection map
  def removeHost(host: String): Unit = client.removeEndpoint(host)

  def send(dest: String, obj: T) {
    if (!client.send(dest, obj)) {
      error("Failed to send slate to destination " + dest)
      if (msClient != null) {
        msClient.sendMessage(NodeRemoveMessage(Host(dest, app.systemHosts(dest))))
      }
    }
  }
}
