package com.walmartlabs.mupd8.messaging
import com.walmartlabs.mupd8.messaging.ActivityStatus.ActivityStatus

class LoadReDistResponseMessage(val id: Int, val transformation: Map[Float, Int], val hostname: String, val status: ActivityStatus) extends Message(MessageKind.LOAD_REDIST_RESPONSE, MessageTransportKind.UNICAST) {

  def getId() = id
  def getKeySpaceTransformation() = transformation
  def getStatus() = status
  def getHostname() = hostname

  override def toString(): String = {
    var builder = new StringBuilder();
    builder.append(id)
    builder.append("_")
    builder.append("(");
    for ((k, v) <- transformation) {
      builder.append(k)
      builder.append("->")
      builder.append(v)
      builder.append(",")
    }
    if (transformation.size > 0) {
      builder.deleteCharAt(builder.size - 1)
    }
    builder.append(")");
    builder.append("_")
    builder.append(hostname)
    builder.append("_")
    builder.append(status.toString())
    builder.toString
  }
}

object LoadReDistResponseMessage {
  def initFromString(msgContent: String): LoadReDistResponseMessage = {
    var comp = msgContent.split("_")
    var id = Integer.parseInt(comp(0))
    var trans = Map[Float, Int]()
    var transformation = comp(1).substring(1, comp(1).length() - 1)
    var transComp = transformation.split(",")
    for (t <- transComp) {
      var tComp = t.split("->")
      var k = tComp(0)
      var v = tComp(1)
      trans += (java.lang.Float.parseFloat(k) -> Integer.parseInt(v))
    }
    var hostname = comp(2)
    var status = ActivityStatus.withName(comp(3))
    new LoadReDistResponseMessage(id, trans, hostname, status)
  }

}

