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

import scala.collection._
import scala.collection.breakOut
import scala.collection.immutable.StringOps
import scala.collection.JavaConverters._
import scala.util.Sorting
import scala.util.parsing.json.JSON
import util.control.Breaks._
import java.util.concurrent._
import java.util.ArrayList
import java.io.{ File, InputStream, OutputStream }
import java.lang.Integer
import java.lang.Number
import java.net._
import org.json.simple._
import org.scale7.cassandra.pelops._
import org.apache.cassandra.thrift.ConsistencyLevel
import org.jboss.netty.buffer.{ ChannelBuffers, ChannelBuffer }
import org.jboss.netty.channel.{ ChannelHandlerContext, Channel }
import org.jboss.netty.handler.codec.oneone.OneToOneEncoder
import org.jboss.netty.handler.codec.replay.ReplayingDecoder
import com.walmartlabs.mupd8.application.binary.Slate
import com.walmartlabs.mupd8.application.binary
import com.walmartlabs.mupd8.compression.CompressionFactory
import com.walmartlabs.mupd8.compression.CompressionService
import com.walmartlabs.mupd8.Misc._
import com.walmartlabs.mupd8.application._
import com.walmartlabs.mupd8.application.statistics.StatisticsBootstrap
import com.walmartlabs.mupd8.application.statistics.StatisticsConstants
import com.walmartlabs.mupd8.application.statistics.MapWrapper
import com.walmartlabs.mupd8.application.statistics.UpdateWrapper
import com.walmartlabs.mupd8.application.statistics.StatisticsBootstrap
import com.walmartlabs.mupd8.application.statistics.StatisticsConstants
import com.walmartlabs.mupd8.application.statistics.MapWrapper
import com.walmartlabs.mupd8.application.statistics.UpdateWrapper
import com.walmartlabs.mupd8.network.common.Decoder.DecodingState
import com.walmartlabs.mupd8.network.client._
import com.walmartlabs.mupd8.network.server._
import com.walmartlabs.mupd8.network.common._
import scala.collection.JavaConversions._
import grizzled.slf4j.Logging
// import miscM._
// import GT._
// import Mupd8Type._
import com.walmartlabs.mupd8.network.common.Decoder.DecodingState._
// import com.walmartlabs.mupd8.network.common.Decoder.DecodingState
import java.nio.ByteBuffer
import java.io.ByteArrayOutputStream

object miscM extends Logging {

  val SLATE_CAPACITY = 1048576 // 1M size
  val INTMAX: Long = Int.MaxValue.toLong
  val HASH_BASE: Long = Int.MaxValue.toLong - Int.MinValue.toLong

  // A SlateUpdater receives a SlateValue.slate of type SlateObject.
  type SlateObject = Object

  def log(s: => { def toString: String }) = {} //println("[" + Thread.currentThread().getName() + "]" + s.toString)

  def str(a: Array[Byte]) = new String(a)

  def argParser(syntax: Map[String, (Int, String)], args: Array[String]): Option[Map[String, List[String]]] = {
    var parseSuccess = true

    def next(i: Int): Option[((String, List[String]), Int)] =
      if (i >= args.length)
        None
      else
        syntax.get(args(i)).filter(i + _._1 < args.length).map { p =>
          ((args(i), (i + 1 to i + p._1).toList.map(args(_))), i + p._1 + 1)
        }.orElse { parseSuccess = false; None }

    val result = unfold(0, next).sortWith(_._1 < _._1)

    parseSuccess = parseSuccess && !result.isEmpty && !(result zip result.tail).exists(p => p._1._1 == p._2._1)
    if (parseSuccess)
      Some(Map.empty ++ result)
    else
      None
  }

  class Later[T] {
    var obj: Option[T] = None
    val sem = new Semaphore(0)
    def get(): T = { sem.acquire(); obj.get }
    def set(x: T) { obj = Option(x); sem.release() }
  }

  def fetchURL(urlStr: String): Option[Array[Byte]] = {
    excToOptionWithLog {
      // XXX: URL class doesn't {en,de}code any url string by itself
      val url = new java.net.URL(urlStr)
      val urlConn = url.openConnection
      urlConn.setRequestProperty("connection", "Keep-Alive")
      urlConn.connect
      val is: InputStream = urlConn.getInputStream
      // XXX: as long as we enforce SLATE_CAPACITY, buffer will not overflow
      val buffer = new java.io.ByteArrayOutputStream(Misc.SLATE_CAPACITY)
      val bbuf = new Array[Byte](8192)
      var c = is.read(bbuf, 0, 8192)
      while (c >= 0) {
        buffer.write(bbuf, 0, c)
        c = is.read(bbuf, 0, 8192)
      }
      is.close
      buffer.toByteArray()
    }
  }

}

import miscM._

trait MapUpdateClass[T] extends OneToOneEncoder with Runnable with Comparable[T] with java.io.Serializable {
  def getKey: Any
}

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
      debug("Server receives: " + destObj)
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

  def self = InetAddress.getLocalHost.getHostName
  info("Host id is " + self)

  def init() {
    server.start()
    client.init()
    hosts.filter(_.compareTo(self) != 0).foreach (host => connectToHost(host))
  }

  private def connectToHost(host: String): Unit = {
    val timeout: Long = java.lang.System.currentTimeMillis + (10000 * java.lang.Math.sqrt(hosts.size)).toLong
    info("Connecting to " + host + " at port " + port)
    while (!client.connect(host, port) && java.lang.System.currentTimeMillis < timeout) {
      java.lang.Thread.sleep(500)
    }
    if (client.isConnected(host))
      info("Connected to " + host + " at port " + port)
    else {
      if (msClient != null) msClient.sendMessage(NodeRemoveMessage(host))
      error("Failed to connect to " + host + " at port " + port)
    }
  }

  // Add host to connection map
  def addHost(host: String): Unit = connectToHost(host)
  // Remove host from connection map
  def removeHost(host: String): Unit = client.disconnect(host)

  def send(dest: String, obj: T) {
    if (!client.send(dest, obj)) {
      if (msClient != null)
        error("Failed to send message to destination " + dest)
        msClient.sendMessage(NodeRemoveMessage(dest))
    }
  }
}

class MapUpdatePool[T <: MapUpdateClass[T]](val poolsize: Int, runtime: AppRuntime, clusterFactory: (T => Unit) => MUCluster[T]) extends Logging {
  case class innerCompare(job: T, key: Any) extends Comparable[innerCompare] {
    override def compareTo(other: innerCompare) = job.compareTo(other.job)
  }
  val ring = runtime.ring

  class ThreadData(val me: Int) {
    val queue = new PriorityBlockingQueue[innerCompare]
    private[MapUpdatePool] var keyInUse: Any = null
    private[MapUpdatePool] var keyQueue = new mutable.Queue[Runnable]
    private[MapUpdatePool] val keyLock = new scala.concurrent.Lock

    val thread = new Thread(run {
      while (true) {
        val item = queue.take()
        if (item.key == null) {
          item.job.run() // This is a mapper job
        } else {
          val (i1, i2) = getPoolIndices(item.key)
          debug("ThreadData: item = " + item + "; item.key = " + item.key + ", index = " + (i1, i2))
          assert(me == i1 || me == i2)
          lock(i1, i2)
          if (attemptQueue(item.job, item.key, i1, i2)) {
            unlock(i1, i2)
          } else {
            keyInUse = item.key
            unlock(i1, i2)
            item.job.run()
            var jobCount = 0
            var currentlyHot = false
            while ({
              lock(i1, i2)
              val work = keyQueue.headOption
              if (work != None) keyQueue.dequeue() else keyInUse = null
              val newPriority = currentlyHot || keyQueue.size > 50
              unlock(i1, i2)
              if (newPriority != currentlyHot) {
                currentlyHot = newPriority
                Thread.currentThread.setPriority(Thread.MAX_PRIORITY)
              }
              val otherItem = if (jobCount % 5 == 4) Option(queue.poll()) else None
              otherItem.map { it => if (it.key == null) put(it.job) else putLocal(it.key, it.job) }
              work map { _.run() }
              jobCount += 1
              work != None
            }) {}
            if (currentlyHot) {
              Thread.currentThread.setPriority(Thread.NORM_PRIORITY)
            }
          }
        }
      }
    }, "MapUpdateThread-" + me)
    thread.start()

    def getSerialQueueSize() = {
      //      keyLock.acquire
      val size = keyQueue.size
      //      keyLock.release
      size
    }
  }

  val pool = 0 until poolsize map { new ThreadData(_) }
  private val rand = new java.util.Random(System.currentTimeMillis)
  val cluster = clusterFactory(p => putLocal(p.getKey, p))
  def init() { cluster.init() }

  def mod(i: Int) = if (i < 0) -i else i

  private val HASH_CONSTANT = 17
  // Get queues in queue for key
  private def getPoolIndices(key: Any) = {
    val fullhash = key.hashCode()
    val hash = fullhash / HASH_CONSTANT //cluster.hosts.size
    val i1 = hash % pool.size
    val i2 = (hash / pool.size) % (pool.size - 1)
    val (m1, m2) = (mod(i1), mod(i2))
    (m1, if (m2 < m1) m2 else m2 + 1)
  }

  def getPreferredPoolIndex(key: Any) = {
    val fullhash = key.hashCode()
    val hash = fullhash / HASH_CONSTANT //cluster.hosts.size
    mod(hash % pool.size)
  }

  private def lock(i1: Int, i2: Int) {
    val (k1, k2) = if (i1 < i2) (i1, i2) else (i2, i1)
    pool(k1).keyLock.acquire()
    if (k1 != k2) pool(k2).keyLock.acquire()
  }

  private def unlock(i1: Int, i2: Int) {
    val (k1, k2) = if (i1 < i2) (i1, i2) else (i2, i1)
    pool(k2).keyLock.release()
    if (k1 != k2) pool(k1).keyLock.release()
  }

  // This method should only be called after acquiring the (i1,i2) locks
  private def attemptQueue(job: Runnable with Comparable[T], key: Any, i1: Int, i2: Int): Boolean = {
    val (p1, p2) = (pool(i1), pool(i2))

    // somehow scala convert keyInUse or key into StringOps, so need to convert to string to call equals
    // o.w. it always returns false
    val b1 = if (p1.keyInUse != null) p1.keyInUse.toString.equals(key.toString) else false
    val b2 = if (p2.keyInUse != null) p2.keyInUse.toString.equals(key.toString) else false
    assert(!b1 || !b2 || b1 == b2)
    if (b1 || b2) {
      val dest = if (b1) p1 else p2
      dest.keyQueue.enqueue(job)
      true
    } else {
      false
    }
  }

  def put(x: T) {
    val a = rand.nextInt(pool.size) //TODO: Do we need to serialize this call?
    val sa = pool(a).keyQueue.size + pool(a).queue.size()

    val destination =
      if (sa > 1) {
        val temp = rand.nextInt(pool.size - 1)
        val b = if (temp < a) temp else temp + 1
        if (pool(b).keyQueue.size + pool(b).queue.size < sa) b else a
      } else a

    pool(destination).queue.put(innerCompare(x, null))
  }

  // Put source into queue
  def putSource(x: T) {
    var a = 0
    var sa = 0
    while ({
      a = rand.nextInt(pool.size) //TODO: Do we need to serialize this call?
      sa = pool(a).keyQueue.size + pool(a).queue.size()
      sa > 50
    }) {
      java.lang.Thread.sleep((sa - 50L) * (sa - 50L) / 25 min 1000)
    }

    pool(a).queue.put(innerCompare(x, null))
  }

  def putLocal(key: Any, x: T) { // TODO : Fix key : Any??
    val (i1, i2) = getPoolIndices(key)
    lock(i1, i2)

    if (!attemptQueue(x, key, i1, i2)) {
      // TODO: HOT conductor check not accurate, use time stamps
      val (p1, p2) = (pool(i1), pool(i2))
      val dest = if (p1.keyQueue.size + p1.queue.size > 1.3 * (p2.keyQueue.size + p2.queue.size)) p2 else p1
      dest.queue.put(innerCompare(x, key))
    }

    unlock(i1, i2)
  }

  def put(key: Any, x: T) {
    val dest = runtime.ring(key)
    if (dest == cluster.self) putLocal(key, x) else cluster.send(dest, x)
  }

  /*
   Since hot conductor is not used, comment it out temporarily.
  // Hot Conductor Queue Status
  val queueStatus = cluster.hosts.map(_ => 0).toArray
  var maxQueueBacklog = 0 // TODO: Make this volatile
  val queueStatusServer = new HttpServer(cluster.port + 1, cluster.hosts.length,
    s => if(s.split('/')(1) == "queuestatus")
           Some { pool.map(p => p.queue.size + p.getSerialQueueSize()).max.toString.getBytes }
         else
           None
    )
  queueStatusServer.start

  val queueStatusUpdater = new Thread(run {
    cluster.hosts.foreach { host =>
      excToOptionWithLog {
        java.lang.Thread.sleep(500)
        if (host.compareTo(cluster.self) != 0) {
          val quote = fetchURL("http://" + host + ":" + (cluster.port + 1) + "/queuestatus")
          quote map(new String(_).toInt) getOrElse(0)
        } else
          pool.map(p => p.queue.size + p.getSerialQueueSize()).max
      } map { p =>
        queueStatus(i) = p
        maxQueueBacklog = queueStatus.max
      }
    }
  }, "queueStatusUpdater")
  //TODO: Uncomment the following line
  //Do we need a thread pool here
  //queueStatusUpdater.start()
  */

}

object GT {
  type Key = Array[Byte]
  type Event = Array[Byte]
  type Priority = Int

  val source: Priority = 96 * 1024
  val normal: Priority = 64 * 1024
  val system: Priority = 0
  type TypeSig = (Int, Int) // AppID, PerformerID
}

import GT._

trait IoPool extends Logging{
  def fetch(name: String, key: Key, next: Option[Array[Byte]] => Unit)
  def write(columnName: String, key: Key, slate: Array[Byte]): Boolean
  def pendingCount = 0
}

class NullPool extends IoPool {
  def fetch(name: String, key: Key, next: Option[Array[Byte]] => Unit) { next(None) }
  def write(columnName: String, key: Key, slate: Array[Byte]): Boolean = true
}

class CassandraPool(
  val hosts: Array[String],
  val port: Int,
  val keyspace: String,
  val getCF: String => String,
  val getTTL: String => Int,
  val compressionCodec: String) extends IoPool {

  val poolName = keyspace //TODO: Change this
  val cluster = new Cluster(hosts.reduceLeft(_ + "," + _), port)
  val cService = CompressionFactory.getService(compressionCodec)
  info("Use compression codec " + compressionCodec)
  val dbIsConnected =
    {
      for (
        keySpaceManager <- Option(Pelops.createKeyspaceManager(cluster));
        _ <- { info("Getting keyspaces from Cassandra Cluster " + hosts.reduceLeft(_ + "," + _) + ":" + port); Some(true) };
        keySpaces <- excToOption(keySpaceManager.getKeyspaceNames.toArray.map(_.asInstanceOf[org.apache.cassandra.thrift.KsDef]));
        _ <- { info("[OK] - Checking for keyspace " + keyspace); Some(true) };
        ks <- keySpaces find (_.getName == keyspace)
      //_ <- {info("[OK] - Checking for column family " + columnFamily) ; Some(true)} ;
      //cfs             <- ks.getCf_defs.toArray find {_.asInstanceOf[org.apache.cassandra.thrift.CfDef].getName == columnFamily}
      ) yield { info("Keyspace " + keyspace + " is found"); true }
    } getOrElse { error("Keyspace " + keyspace + " is not found. Terminating Mupd8..."); false }

  if (!dbIsConnected) java.lang.System.exit(1)

  Pelops.addPool(poolName, cluster, keyspace)
  val selector = Pelops.createSelector(poolName) // TODO: Should this be per thread?
  val pool = new java.util.concurrent.ThreadPoolExecutor(10, 50, 5, TimeUnit.SECONDS, new LinkedBlockingQueue[Runnable]) // TODO: We can drop events unless we have a Rejection Handler or LinkedQueue

  def fetch(name: String, key: Key, next: Option[Array[Byte]] => Unit) {
    pool.submit(run {
      val start = java.lang.System.nanoTime()
      val col = excToOption(selector.getColumnFromRow(getCF(name), Bytes.fromByteArray(key), Bytes.fromByteArray(name.getBytes), ConsistencyLevel.QUORUM))
      debug("Fetch " + (java.lang.System.nanoTime() - start) / 1000000 + " " + name + " " + str(key))
      next(col.map { col =>
        assert(col != null)
        val ba = Bytes.fromByteBuffer(col.value).toByteArray
        cService.uncompress(ba)
      })
    })
  }

  def write(columnName: String, key: Key, slate: Array[Byte]) = {
    val compressed = cService.compress(slate)
    val mutator = Pelops.createMutator(poolName)
    mutator.writeColumn(
      getCF(columnName),
      Bytes.fromByteArray(key),
      mutator.newColumn(Bytes.fromByteArray(columnName.getBytes), Bytes.fromByteArray(compressed), getTTL(columnName)))
    excToOptionWithLog { mutator.execute(ConsistencyLevel.QUORUM) } != None
  }

  override def pendingCount = pool.getQueue.size + pool.getActiveCount
}

class Node[T](val item: T, var next: Node[T] = null, var prev: Node[T] = null)

class DLList[T] {
  var front: Node[T] = null
  var last: Node[T] = null
  final def add(node: Node[T]) {
    assert(node.next == null && node.prev == null)
    if (front != null) front.prev = node
    node.next = front
    front = node
    if (last == null) {
      last = front
      assert(front.next == null)
    }
  }

  final def remove(node: Node[T]) {
    if (node.prev == null) {
      assert(node == front)
      front = node.next
    } else {
      node.prev.next = node.next
    }
    if (node.next == null) {
      assert(node == last)
      last = node.prev
    } else {
      node.next.prev = node.prev
    }
    node.prev = null
    node.next = null
  }
}


class SlateValue(val slate: SlateObject, val keyRef: Node[String], var dirty: Boolean = true)

class SlateCache(val io: IoPool, val usageLimit: Long) {
  println("Creating new SlateCache with IoPool " + io + " and usageLimit " + usageLimit);
  private val lock = new scala.concurrent.Lock
  private val table = new mutable.HashMap[String, SlateValue]()
  private val lru = new DLList[String]

  private var currentUsage: Long = 0
  private val objOverhead: Long = 32

  def buildKey(name: String, key: Key) = name + "~~~" + str(key)

  def getSlate(akey: (String, Key)) = {
    val skey = buildKey(akey._1, akey._2)
    lock.acquire()
    val item = table.get(skey)
    item map { p =>
      lru.remove(p.keyRef)
      lru.add(p.keyRef)
    }
    lock.release()
    item.map(_.slate)
  }

  def waitForSlate(akey: (String, Key), action: SlateObject => Unit, updater : binary.SlateUpdater, slateBuilder : binary.SlateBuilder, doSafePut : Boolean = true) {
    val skey = buildKey(akey._1, akey._2)
    lock.acquire()
    val item = table.get(skey)
    item map { p =>
      lru.remove(p.keyRef)
      lru.add(p.keyRef)
    }
    lock.release()
    item map { p =>
      action(p.slate)
    } getOrElse {
      while (io.pendingCount > 200) java.lang.Thread.sleep(100)
      def slateFromBytes(p:Array[Byte]) = { slateBuilder.toSlate(p) }
      if( doSafePut ) {
        io.fetch(akey._1, akey._2, p => action(safePut(akey, p.map {slateFromBytes}.getOrElse(updater.getDefaultSlate()))))
      } else {
        io.fetch(akey._1, akey._2, p => action(p.map {slateFromBytes}.getOrElse(updater.getDefaultSlate())))
      }
    }
  }

  @inline private def bytesConsumed(skey : String, item : SlateObject) = 1

  private def unLockedPut(skey: String, value: SlateObject, dirty: Boolean = true) = {
    val newVal = new SlateValue(value, new Node(skey), dirty)
    lru.add(newVal.keyRef)
    currentUsage += bytesConsumed(skey, value)
    val oldVal = table.put(skey, newVal)
    assert(dirty || oldVal == None)
    oldVal map { p =>
      lru.remove(p.keyRef)
      currentUsage -= bytesConsumed(skey, value)
    }

    var count = 0
    while (currentUsage > usageLimit && lru.last != lru.front && count < 10) {
      //      assert(last != null && lru != null)
      val item = lru.last.item
      lru.remove(lru.last)
      table.get(item) map { p =>
        if (p.dirty) {
          // Potential infinite loop here, which is why we are forced to use count
          lru.add(p.keyRef)
        } else {
          currentUsage -= bytesConsumed(skey, value)
          table.remove(item)
        }
      } getOrElse {
        assert(false)
      }
      count += 1
    }
    newVal
  }

  // To be used only by replaceSlate
  def put(akey: (String, Key), value: SlateObject) {
    val skey = buildKey(akey._1, akey._2)
    lock.acquire()
    unLockedPut(skey, value)
    lock.release()
  }

  private def safePut(akey: (String, Key), value: SlateObject) = {
    val skey = buildKey(akey._1, akey._2)
    lock.acquire()
    val slate = table get (skey) getOrElse unLockedPut(skey, value, false)
    lock.release()
    slate.slate
  }

  def getDirtyItems() = {
    lock.acquire()
    val retVal = table.filter(_._2.dirty).toList
    lock.release()
    retVal
  }

  def getAllItems() = {
    lock.acquire()
    val retVal = table.toList
    lock.release()
    retVal
  }

}

object Mupd8Type extends Enumeration {
  type Mupd8Type = Value
  val Source, Mapper, Updater = Value
}
import Mupd8Type._

case class Performer(name: String,
  pubs: Vector[String],
  subs: Vector[String],
  mtype: Mupd8Type,
  ptype: Option[String],
  jclass: Option[String],
  wrapperClass: Option[String],
  slateBuilderClass: Option[String],
  workers: Int,
  cf: Option[String],
  ttl: Int,
  copy: Boolean)

object loadConfig {

  def isTrue(value: Option[String]): Boolean = {
    (value != null) && (value != None) && (value.get.toLowerCase != "false") && (value.get.toLowerCase != "off") && (value.get != "0")
  }

  def convertPerformers(pHashMap: java.util.HashMap[String, org.json.simple.JSONObject]) = {
    val performers = pHashMap.asScala.toMap
    def convertStrings(list: java.util.List[String]): Vector[String] = {
      if (list == null) Vector() else list.asScala.toArray.map(p => p)(breakOut)
    }

    performers.map(p =>
      Performer(
        name = p._1,
        pubs = convertStrings(p._2.get("publishes_to").asInstanceOf[ArrayList[String]]),
        subs = convertStrings(p._2.get("subscribes_to").asInstanceOf[ArrayList[String]]),
        mtype = Mupd8Type.withName(p._2.get("mupd8_type").asInstanceOf[String]),
        ptype = Option(p._2.get("type").asInstanceOf[String]),
        jclass = Option(p._2.get("class").asInstanceOf[String]),
        wrapperClass = { Option(p._2.get("wrapper_class").asInstanceOf[String]) },
        slateBuilderClass = Option(p._2.get("slate_builder").asInstanceOf[String]),
        workers = if (p._2.get("workers") == null) 1.toInt else p._2.get("workers").asInstanceOf[Number].intValue(),
        cf = Option(p._2.get("column_family").asInstanceOf[String]),
        ttl = if (p._2.get("slate_ttl") == null) Mutator.NO_TTL else p._2.get("slate_ttl").asInstanceOf[Number].intValue(),
        copy = isTrue(Option(p._2.get("clone").asInstanceOf[String]))))(breakOut)
  }

}

// A factory that constructs a SlateUpdater that runs an Updater.
// The SlateUpdater expects to be accompanied by a ByteArraySlateBuilder as
// its SlateBuilder so that the slate object indeed stays the raw byte[].
class UpdaterFactory[U <: binary.Updater](val updaterType : Class[U]) {
  val updaterConstructor = updaterType.getConstructor(classOf[Config], classOf[String])
  def construct(config : Config, name : String) : binary.SlateUpdater = {
    val updater = updaterConstructor.newInstance(config, name)
    val updaterWrapper = new binary.SlateUpdater() {
      override def getName() = updater.getName()
      override def update(util : binary.PerformerUtilities, stream : String, k : Array[Byte], v : Array[Byte], slate : SlateObject) = {
        updater.update(util, stream, k, v, slate.asInstanceOf[Array[Byte]])
      }
      override def getDefaultSlate() : Array[Byte] = Array[Byte]()
    }
    updaterWrapper
  }
}

class AppStaticInfo(val configDir: Option[String], val appConfig: Option[String], val sysConfig: Option[String], val loadClasses: Boolean, statistics: Boolean, elastic: Boolean) extends Logging {
  assert(appConfig.size == sysConfig.size && appConfig.size != configDir.size)
  val config = configDir map { p => new application.Config(new File(p)) } getOrElse new application.Config(sysConfig.get, appConfig.get)
  val performers = loadConfig.convertPerformers(config.workerJSONs)
  val statusPort = Option(config.getScopedValue(Array("mupd8", "mupd8_status", "http_port"))).getOrElse(new Integer(6001)).asInstanceOf[Number].intValue()
  val performerName2ID = Map(performers.map(_.name).zip(0 until performers.size): _*)
  val edgeName2IDs = performers.map(p => p.subs.map((_, performerName2ID(p.name)))).flatten.groupBy(_._1).mapValues(_.map(_._2))
  var performerArray: Array[binary.Performer] = new Array[binary.Performer](performers.size)

  val performerFactory: Vector[Option[() => binary.Performer]] = if (loadClasses) (
    0 until performers.size map { i =>
      val p = performers(i)
      var isMapper = false
      // the wrapper class that wraps a performer instance
      var wrapperClass: Option[String] = null
      info("Loading ... " + p.name + " " + p.mtype)
      val constructor : Option[() => binary.Performer] = p.mtype match {
        case Mapper => {
          isMapper = true; wrapperClass = p.wrapperClass;
          p.jclass.map(Class.forName(_)).map { m =>
            if (classOf[binary.Mapper].isAssignableFrom(m)) {
              val mapperConstructor = m.asSubclass(classOf[binary.Mapper]).getConstructor(config.getClass, "".getClass)
              () => mapperConstructor.newInstance(config, p.name)
            } else {
              val msg = "Mapper "+p.name+" uses class "+m.getName()+" that is not assignable to "+classOf[binary.Mapper].getName()
              error(msg)
              throw new ClassCastException(msg)
            }
          }
        }
        case Updater => {
          isMapper = false; wrapperClass = p.wrapperClass;
          p.jclass.map(Class.forName(_)).map { u =>
            if (classOf[binary.SlateUpdater].isAssignableFrom(u)) {
              if (p.slateBuilderClass.isEmpty) {
                val msg = "Updater "+p.name+" uses a SlateUpdater class but does not specify a corresponding slate_builder."
                error(msg)
                info("An updater with no slate_builder may use a class Updater but not SlateUpdater.");
                throw new ClassCastException("Updater "+p.name+" does not define slate_builder but class "+u.getName()+" implements SlateUpdater, not Updater.")
              }
              val updaterConstructor = u.asSubclass(classOf[binary.SlateUpdater]).getConstructor(config.getClass, "".getClass)
              () => updaterConstructor.newInstance(config, p.name)
            } else if (classOf[binary.Updater].isAssignableFrom(u)) {
              val updaterFactory = new UpdaterFactory(u.asSubclass(classOf[binary.Updater]))
              () => updaterFactory.construct(config, p.name)
            } else {
              val msg = "Updater "+p.name+" uses class "+u+" that is not assignable to "+classOf[binary.Updater].getName()
              error(msg)
              throw new ClassCastException(msg)
            }
          }
        }
        case _ => None
      }
      constructor.map { performerConstructor =>
        val needToCollectStatistics = statistics | elastic
        val wrappedPerformer =
          if (statistics) {
            val wrapperClassname = wrapperClass match {
              case None => StatisticsConstants.DEFAULT_PRE_PERFORMER
              case Some(x) => x
            }
            var constructor = Class.forName(wrapperClassname).asInstanceOf[Class[com.walmartlabs.mupd8.application.statistics.PrePerformer]].getConstructor("".getClass());
            val prePerformer = constructor.newInstance(p.name)
            if (isMapper) {
              new MapWrapper(performerConstructor().asInstanceOf[binary.Mapper], prePerformer)
            } else {
              new UpdateWrapper(performerConstructor().asInstanceOf[binary.SlateUpdater], prePerformer)
            }
          } else {
            performerConstructor()
          }

        performerArray(i) = wrappedPerformer

        if (p.copy) {
          //        if ((p.name == "fbEntityProcessor")||(p.name == "interestStatsFetcher")) {
          () => { log("Building object " + p.name); wrappedPerformer }
        } else {
          val obj = wrappedPerformer
          () => obj
        }
      }
    })(breakOut)
  else Vector()

  val slateBuilderFactory: Vector[Option[() => binary.SlateBuilder]] = if (loadClasses) (
    0 until performers.size map { i =>
      val p = performers(i)
      val slateBuilder = p.mtype match {
        case Updater => {
          // TODO Keep enough state to detect the error case: SlateUpdater but no SlateBuilder specified.
          val slateBuilderClass = p.slateBuilderClass.map(Class.forName(_)).getOrElse(classOf[binary.ByteArraySlateBuilder])
          val castSlateBuilderClass = try {
            slateBuilderClass.asSubclass(classOf[binary.SlateBuilder])
          } catch {
            case e : ClassCastException => {
              error("SlateBuilder class " + slateBuilderClass.getName() + " for updater " + p.name + " is not an implementation of " + classOf[binary.SlateBuilder].getName()+" as required: ", e)
              throw e
            }
          }
          val slateBuilderConstructor = castSlateBuilderClass.getConstructor(config.getClass, "".getClass)
          Some(() => { slateBuilderConstructor.newInstance(config, p.name) })
        }
        case _ => None
      }
      slateBuilder
    })(breakOut)
  else Vector()

  val cassPort = config.getScopedValue(Array("mupd8", "slate_store", "port")).asInstanceOf[Number].intValue()
  val cassKeySpace = config.getScopedValue(Array("mupd8", "slate_store", "keyspace")).asInstanceOf[String]
  val cassHosts = config.getScopedValue(Array("mupd8", "slate_store", "hosts")).asInstanceOf[ArrayList[String]].asScala.toArray
  val cassColumnFamily = config.getScopedValue(Array("mupd8", "application")).asInstanceOf[java.util.HashMap[String, java.lang.Object]].asScala.toMap.head._1
  val cassWriteInterval = Option(config.getScopedValue(Array("mupd8", "slate_store", "write_interval"))) map { _.asInstanceOf[Number].intValue() } getOrElse 15
  val slateCacheCount = Option(config.getScopedValue(Array("mupd8", "slate_store", "slate_cache_count"))) map { _.asInstanceOf[Number].intValue() } getOrElse 1000
  val compressionCodec = Option(config.getScopedValue(Array("mupd8", "slate_store", "compression"))).getOrElse("gzip").asInstanceOf[String].toLowerCase

  var systemHosts: IndexedSeq[String] = null
  val javaClassPath = Option(config.getScopedValue(Array("mupd8", "java_class_path"))).getOrElse("share/java/*").asInstanceOf[String]
  val javaSetting = Option(config.getScopedValue(Array("mupd8", "java_setting"))).getOrElse("-Xmx200M -Xms200M").asInstanceOf[String]

  val sources = Option(config.getScopedValue(Array("mupd8", "sources"))).map {
    x => x.asInstanceOf[java.util.List[org.json.simple.JSONObject]]
  }.getOrElse(new java.util.ArrayList[org.json.simple.JSONObject]())

  val messageServerHost = Option(config.getScopedValue(Array("mupd8", "messageserver", "host")))
  val messageServerPort = Option(config.getScopedValue(Array("mupd8", "messageserver", "port")))

  def internalPort = statusPort + 100;
}

object PerformerPacket {
  @inline def getKey(pid: Int, key: Key): String = pid.toString + "?/%%%>*" + str(key)
}

case class PerformerPacket(pri: Priority,
                           pid: Int,
                           key: Key,
                           event: Event,
                           stream: String, // This field can be replaced by source performer ID
                           appRun: AppRuntime) extends MapUpdateClass[PerformerPacket] with Logging {
  override def getKey = PerformerPacket.getKey(pid, key)

  override def compareTo(other: PerformerPacket) = pri.compareTo(other.pri)

  override def toString = "{" + pri + "," + pid + "," + str(key) + "," + str(event) + "," + stream + "}"

  // Treat this as a static method, do not touch "this", use msg
  override protected def encode(channelHandlerContext: ChannelHandlerContext, channel: Channel, msg: AnyRef): AnyRef = {
    log("Invoked encoder")
    if (msg.isInstanceOf[PerformerPacket]) {
      val packet = msg.asInstanceOf[PerformerPacket]
      val size: Int = 4 + 4 + 4 + packet.key.length + 4 + packet.event.length + 4 + packet.stream.length
      val buffer: ChannelBuffer = ChannelBuffers.buffer(size)
      buffer.writeInt(packet.pri)
      buffer.writeInt(packet.pid)
      buffer.writeInt(packet.key.length)
      buffer.writeBytes(packet.key)
      buffer.writeInt(packet.event.length)
      buffer.writeBytes(packet.event)
      buffer.writeInt(packet.stream.length)
      buffer.writeBytes(packet.stream.getBytes) // TODO: Stream name encoding assumption
      buffer
    } else
      msg
  }

  def run() {
    def execute(x: => Unit) {
      val result = excToOptionWithLog(x)
      if (result == None) log("Bad exception Bro")
    }
    val performer = appRun.app.performers(pid)
    val name = performer.name

    val cache = appRun.getTLS(pid, key).slateCache
    val optSlate = if (performer.mtype == Updater) Some {
      val s = cache.getSlate((name, key))
      s map {
        p => debug("Succeeded for " + name + "," + new String(key) + " " + p)
      } getOrElse {
        debug("Failed fetch for " + name + "," + new String(key))
        // Re-introduce self after issuing a read
        cache.waitForSlate((name,key),_ => appRun.pool.put(new StringOps(this.getKey), this), appRun.getUpdater(pid, key), appRun.getSlateBuilder(pid))
      }
      s
    } else
      None

    if (optSlate != Some(None)) {
      val slate = optSlate.flatMap(p => p)

      slate.map(s =>
        debug("Update now " + "DBG for performer " + name + " Key " + str(key) + " \nEvent " + str(event) + "\nSlate " + s.toString()))

      val tls = appRun.getTLS
      tls.perfPacket = this
      tls.startTime = java.lang.System.nanoTime()
      (performer.mtype, tls.unifiedUpdaters(pid)) match {
        case (Updater, true) => execute(appRun.getUnifiedUpdater(pid).update(tls, stream, key, Array(event), Array(slate.get)))
        case (Updater, false) => execute(appRun.getUpdater(pid).update(tls, stream, key, event, slate.get))
        case (Mapper, false) => execute(appRun.getMapper(pid).map(tls, stream, key, event))
      }
      tls.perfPacket = null
      tls.startTime = 0
      debug("Executed " + performer.mtype.toString + " " + name)
    }
  }
}

class Decoder(val appRun: AppRuntime) extends ReplayingDecoder[DecodingState](PRIORITY) {
  var pri: Priority = -1
  var pid: Int = -1
  var key: Key = Array()
  var event: Event = Array()
  var stream: Array[Byte] = Array()

  reset()
  private def reset() {
    checkpoint(PRIORITY)
    pri = -1
    pid = -1
    key = Array()
    event = Array()
    stream = Array()
  }

  protected def decode(ctx: ChannelHandlerContext, channel: Channel, buffer: ChannelBuffer, stateParam: DecodingState): AnyRef = {
    var p: PerformerPacket = null
    var state = stateParam

    do {
      //      (state.## : @scala.annotation.switch) match {
      state match {
        case PRIORITY =>
          pri = buffer.readInt
          checkpoint(PERFORMERID)
        case PERFORMERID =>
          pid = buffer.readInt
          checkpoint(KEY_LENGTH)
        case KEY_LENGTH =>
          val keyLen = buffer.readInt
          if (keyLen < 0) {
            throw new Exception("Invalid key size")
          }
          key = new Array[Byte](keyLen)
          checkpoint(KEY)
        case KEY =>
          buffer.readBytes(key, 0, key.length)
          checkpoint(EVENT_LENGTH)
        case EVENT_LENGTH =>
          val eventLen = buffer.readInt
          if (eventLen < 0) {
            throw new Exception("Invalid event size")
          }
          event = new Array[Byte](eventLen)
          checkpoint(EVENT)
        case EVENT =>
          buffer.readBytes(event, 0, event.length)
          checkpoint(STREAM_LENGTH)
        case STREAM_LENGTH =>
          val streamLen = buffer.readInt
          if (streamLen < 0) {
            throw new Exception("Invalid stream size")
          }
          stream = new Array[Byte](streamLen)
          checkpoint(STREAM)
        case STREAM =>
          buffer.readBytes(stream, 0, stream.length)
          p = PerformerPacket(pri, pid, key, event, str(stream), appRun)
          reset()
        case _ =>
          throw new Exception("Unknown decoding state: " + state)
      }
      state = getState
    } while (state != PRIORITY)
    //    try { return p } finally { reset() }
    p
  }
}

class TLS(appRun: AppRuntime) extends binary.PerformerUtilities with Logging {
  val objects = appRun.app.performerFactory.map(_.map(_.apply()))
  // val slateCache = new SlateCache(appRun.storeIo, appRun.slateRAM / appRun.pool.poolsize)
  val slateCache = new SlateCache(appRun.storeIo, appRun.app.slateCacheCount)
  val queue = new PriorityBlockingQueue[Runnable]
  var perfPacket: PerformerPacket = null
  var startTime: Long = 0

  val unifiedUpdaters: Set[Int] =
    (for (
      (oo, i) <- objects zipWithIndex;
      o <- oo;
      if excToOption(o.asInstanceOf[binary.UnifiedUpdater]) != None
    ) yield i)(breakOut)

  override def publish(stream: String, key: Array[Byte], event: Array[Byte]) {
    val app = appRun.app
    debug("TLS::Publish: Publishing to " + stream + " Key " + str(key) + " event " + str(event))
    app.edgeName2IDs.get(stream).map(_.foreach(
      pid => {
        debug("TLS::publish: Publishing to " + app.performers(pid).name)
        val packet = PerformerPacket(normal, pid, key, event, stream, appRun)
        if (app.performers(pid).mtype == Mapper)
          appRun.pool.put(packet)
        else
          appRun.pool.put(new StringOps(packet.getKey), packet)
      })).getOrElse(log("Bad Stream name" + stream))
  }

  //  import com.walmartlabs.mupd8.application.SlateSizeException
  @throws(classOf[SlateSizeException])
  override def replaceSlate(slate: SlateObject) {
    // TODO: Optimize replace slate to avoid hash table look ups
    val name = appRun.app.performers(perfPacket.pid).name
    assert(appRun.app.performers(perfPacket.pid).mtype == Updater)
    val cache = appRun.getTLS(perfPacket.pid, perfPacket.key).slateCache
    debug("replaceSlate " + appRun.app.performers(perfPacket.pid).name + "/" + str(perfPacket.key) + " Oldslate " + (cache.getSlate((name,perfPacket.key)).get).toString() + " Newslate " + slate.toString())
    cache.put((name, perfPacket.key), slate)
  }
}

class AppRuntime(appID: Int,
                 poolsize: Int,
                 val app: AppStaticInfo,
                 useNullPool: Boolean = false) extends Logging {
  private val sourceThreads: mutable.ListBuffer[(String, List[java.lang.Thread])] = new mutable.ListBuffer
  val hostUpdateLock = new Object

  def initMapUpdatePool(poolsize: Int, runtime: AppRuntime, clusterFactory: (PerformerPacket => Unit) => MUCluster[PerformerPacket]): MapUpdatePool[PerformerPacket] =
    new MapUpdatePool[PerformerPacket](poolsize, runtime, clusterFactory)


  val msClient: MessageServerClient = if (app.messageServerHost != None && app.messageServerPort != None) {
    new MessageServerClient(app.messageServerHost.get.asInstanceOf[String], app.messageServerPort.get.asInstanceOf[Number].intValue(), 1000)
  } else {
    error("AppRuntime error: message server host name or port is empty")
    null
  }

  // start local server socket
  val localMessageServer = if (app.messageServerPort != None) {
    new Thread(new LocalMessageServer(app.messageServerPort.get.asInstanceOf[Number].intValue() + 1, this), "LocalMessageServer")
  } else {
    error("AppRuntime error: local message server port is None")
    null
  }
  if (localMessageServer != null) localMessageServer.start
  Thread.sleep(200) // Give local Message Server sometime to start

  /* generate Hash Ring */
  private var _ring: HashRing = null
  def ring = _ring // getter
  def ring_= (r: HashRing2): Unit = _ring = new HashRing(r.hash) // setter
  def ring_= (hash: IndexedSeq[String]): Unit = _ring = new HashRing(hash)

  // Try to Register host to message server and get updated hash ring from message server
  // even if hash ring is generated from system hosts already
  while (!msClient.sendMessage(NodeJoinMessage(InetAddress.getLocalHost.getHostName))) {
    info("Connecting to message server failed")
    Thread.sleep(500)
  }

  while (ring == null) {
    info("Waiting for hash ring")
    // TODO: change to a graceful way to wait
    Thread.sleep(500)
  }
  info("Update ring from Message server")

  if (ring == null) error("AppRuntime: No hash ring either from config file or message server")

  val pool = initMapUpdatePool(poolsize, this,
    action => new MUCluster[PerformerPacket](app, app.statusPort + 100,
                                             PerformerPacket(0, 0, Array(), Array(), "", this),
                                             () => { new Decoder(this) },
                                             action, msClient))

  val storeIo = if (useNullPool) new NullPool
                else new CassandraPool(app.cassHosts,
                                       app.cassPort,
                                       app.cassKeySpace,
                                       p => (app.performers(app.performerName2ID(p)).cf).getOrElse(app.cassColumnFamily),
                                       p => app.performers(app.performerName2ID(p)).ttl,
                                       app.compressionCodec)

  val slateRAM: Long = Runtime.getRuntime.maxMemory / 5
  info("Memory available for use by Slate Cache is " + slateRAM + " bytes")
  val slateBuilders = app.slateBuilderFactory.map(_.map(_.apply()))
  private val threadMap: Map[Long, TLS] = pool.pool.map(_.thread.getId -> new TLS(this))(breakOut)
  private val threadVect: Vector[TLS] = (threadMap map { _._2 })(breakOut)

  def getSlate(key: (String, Key)) = {
    val performerId = app.performerName2ID(key._1)
    val host = ring(new StringOps(PerformerPacket.getKey(performerId, key._2)))
    assert(host.compareTo(pool.cluster.self) == 0 || host.compareTo("localhost") == 0 || host.compareTo("127.0.0.1") == 0)
    val future = new Later[SlateObject]
    getTLS(performerId, key._2).slateCache.waitForSlate(key, future.set(_), getUpdater(performerId, key._2), getSlateBuilder(performerId), false)
    val bytes = new ByteArrayOutputStream()
    getSlateBuilder(performerId).toBytes(future.get(), bytes)
    Option(bytes.toByteArray())
  }

  val maxWorkerCount = 2 * Runtime.getRuntime.availableProcessors
  val slateURLserver = new HttpServer(app.statusPort, maxWorkerCount, s => {
    val decoded = java.net.URLDecoder.decode(s, "UTF-8")
    val tok = decoded.split('/')
    if (tok(1) == "favicon.ico") None
    else if (tok(2) == "status") {
      excToOptionWithLog {
        Some(threadVect.map { p =>
          val perfPacket = p.perfPacket // Copy to local variable to avoid race condition
          if (perfPacket != null) app.performers(perfPacket.pid).name + ":" + app.performers(perfPacket.pid).mtype else "Waiting"
        }.
          groupBy(p => p).map(p => (p._1, p._2.size)).toList.
          sortWith(_._2 >= _._2).
          map(x => x._1 + (x._1.length to 50 map (_ => " ")).foldLeft("")(_ + _) + " " + x._2).foldLeft("")(_ + _ + "\n") +
          {
            val poolsizes = pool.pool.map(p => (p.queue.size, p.getSerialQueueSize())).sortWith { case ((a, b), (c, d)) => a + b < c + d }
            val num = (10 :: poolsizes.size / 2 :: Nil).min
            val currTime = java.lang.System.nanoTime()
            val startTimes = threadVect.map(_.startTime).filter(_ != 0)
            val nonHotThreads = poolsizes filter { _._2 < 2 } map { _._1.toFloat }
            val executionTime = if (startTimes.size != 0)
              (currTime - startTimes.sum / startTimes.size) / 500000 // expected exec time is twice the current exec time
            else
              0
            "\nAvg Queue size: " + poolsizes.map { case (a, b) => a + b }.sum.toFloat / poolsizes.size +
              "\nAvg Queue size w/o hot conductor: " + (if (nonHotThreads.isEmpty) "0.0" else nonHotThreads.sum / nonHotThreads.size) +
              "\nMin size: " + poolsizes.take(num) +
              "\nMax size: " + poolsizes.reverse.take(num) +
              "\nAverage estimated exec time milliseconds: " + executionTime +
              "\nPending IOs: " + storeIo.pendingCount +
              "\n"
          }).get.getBytes
      }
    } else if (tok(2) == "slate") {
      if (tok.length != 6) {
        None
      } else {
        val key: (String, Key) = (tok(4), tok(5).map(_.toByte).toArray)
        val poolKey = PerformerPacket.getKey(app.performerName2ID(key._1), key._2)
        val dest = InetAddress.getByName(ring(new StringOps(poolKey))).getHostName
        if (pool.cluster.self.compareTo(dest) == 0 || dest.compareTo("localhost") == 0 || dest.compareTo("127.0.0.1") == 0)
          getSlate(key)
        else {
          val slate = fetchURL("http://" + dest + ":" + (app.statusPort + 300) + s)
          if (slate == None) {
            // TODO: send remove messageServer
            warn("Can't reach dest " + dest)
          }
          slate
        }
      }
    } else {
      // This section currently handle varnish probe -- /mupd8/config/app
      // TODO: how to handle other requests?
      Some("{}".getBytes)
    }
  })
  slateURLserver.start

  // We need a separate slate server that does not redirect to prevent deadlocks
  val slateServer = new HttpServer(app.statusPort + 300, maxWorkerCount, s => {
    val tok = java.net.URLDecoder.decode(s, "UTF-8").split('/')
    getSlate((tok(4), tok(5).map(_.toByte).toArray))
  })
  slateServer.start

  def startSource(sourcePerformer: String, sourceClassName: String, sourceClassParams: java.util.List[String]) = {
    def initiateWork(performerID: Int, stream: String, data: Mupd8DataPair) = {
      // TODO: Sleep if the pending IO count is in excess of ????
      // Throttle the Source if we have a hot conductor
      // var exponent = 20;
      // while (pool.maxQueueBacklog > 10000) {
      //   if (exponent >= 28) exponent = 10 else exponent += 1
      //   val sleepTime = 1 << exponent;
      //   Thread.sleep(sleepTime)
      // }
      if (data._key.size <= 0) {
        error("No key/Invalid key in Source Event " + excToOption(str(data._value)))
      } else {
        pool.putSource(PerformerPacket(
          source,
          performerID,
          data._key.getBytes(),
          data._value,
          stream,
          this))
      }
    }

    class SourceThread(sourceClassName: String,
                       sourceParams: java.util.List[String],
                       continuation: Mupd8DataPair => Unit) extends Runnable with Logging {
      override def run() = {
        val cls = Class.forName(sourceClassName)
        val ins = cls.getConstructor(Class.forName("java.util.List")).newInstance(sourceParams).asInstanceOf[com.walmartlabs.mupd8.application.Mupd8Source]
        breakable {
          while (true) {
            try {
              if (ins.hasNext()) {
                val data = ins.getNextDataPair();
                continuation(data)
              } else break() // end source thread at first no next returns
            } catch {
              case e: Exception => error("SourceThread: hit exception", e)
            } // catch everything to keep source running
          }
        }
      }
    }

    // TODO : Behavior for multiple performers is most probably wrong!!
    val threads = for (
      perfID <- app.performerName2ID.get(sourcePerformer).toList;
      if app.performers(perfID).mtype == Source;
      edgeName <- app.performers(perfID).pubs;
      destID <- app.edgeName2IDs(edgeName);
      input <- excToOptionWithLog(new SourceThread(sourceClassName, sourceClassParams, initiateWork(destID, edgeName, _)))
    ) yield (new java.lang.Thread(input, "SourceReader:" + sourcePerformer + ":" + (sourceClassParams.asScala mkString ":")))

    if (!threads.isEmpty)
      sourceThreads += ((sourcePerformer, threads))
    else
      error("Unable to set up source for " + sourcePerformer + " server class " + sourceClassName + " with params: " + sourceClassParams)

    threads foreach { _.start() }
    !threads.isEmpty
  }

  val writerThread = new Thread(run {
    val interval = app.cassWriteInterval * 1000 / threadVect.size
    while (true) {
      threadVect foreach { tls =>
        {
          val target = java.lang.System.currentTimeMillis + interval
          val items = tls.slateCache.getDirtyItems
          // println("num of dirty items " + items.length)
          items.zipWithIndex.foreach {
            case ((key, item), i) => {
              writeSlateToCassandra((key, item))
              val sleepTime = (target - java.lang.System.currentTimeMillis) / (items.size - i)
              if (sleepTime > 0) java.lang.Thread.sleep(sleepTime)
            }
          }
          java.lang.Thread.sleep(10)
        }
      }
    }
  }, "writerThread")
  writerThread.start()

  // This step should be the last step in the initialization (it brings up the MapUpdatePool sockets)
  // to avoid exposing the JVM to external input before it is ready
  pool.init()


  //COMMMENT:  A utility method to write slate to cassandra.
  def writeSlateToCassandra(item: (String, SlateValue)) = {
    val key = item._1
    val slateVal = item._2
    val colname = key.take(key.indexOf("~~~"))
    val slateByteStream = new ByteArrayOutputStream()
    debug("Serializing cached slate for updater "+colname+" to Cassandra: "+slateVal.slate)
    var suc = getSlateBuilder(app.performerName2ID(colname)).toBytes(slateVal.slate, slateByteStream)
    // TODO: .getBytes may not work the way you want it to!! Encoding issues!
    suc &&= storeIo.write(colname, key.drop(colname.length + 3).getBytes, slateByteStream.toByteArray())
    // val suc = storeIo.write(colname, key.drop(colname.length + 3).getBytes, slateVal.slate.toBytes())
    if (suc) {
      slateVal.dirty = false
      log("Wrote record for " + colname + " " + key)
    } else {
      slateVal.dirty = true
      log("Failed to write record for " + colname + " " + key)
    }
  }

  def getSlateBuilder(pid: Int) = slateBuilders(pid).get

  // The getTLS method, and therefore the getFoo(pid) methods that depend on it,
  // succeed only for threads in threadMap (namely, MapUpdatePool threads).
  // Other threads need to use the getFoo(pid, key) form instead to "borrow" a
  // Foo from one of the threadMap threads (chosen by key).
  //
  def getTLS = threadMap(Thread.currentThread().getId)

  def getMapper(pid: Int) = getTLS.objects(pid).get.asInstanceOf[binary.Mapper]

  def getUpdater(pid: Int) = getTLS.objects(pid).get.asInstanceOf[binary.SlateUpdater]
  def getUpdater(pid: Int, key: Key) = getTLS(pid, key).objects(pid).get.asInstanceOf[binary.SlateUpdater]
  def getUnifiedUpdater(pid: Int) = getTLS.objects(pid).get.asInstanceOf[binary.UnifiedUpdater]

  // This hash function must be the same as the MapUpdatePool and different from SlateCache
  def getTLS(pid: Int, key: Key) =
    threadVect(pool.getPreferredPoolIndex(PerformerPacket.getKey(pid, key)))
}

class MasterNode(args: Array[String], config: AppStaticInfo, shutdown: Boolean) extends Logging {
  val targetNodes = config.systemHosts
  info("Target nodes are " + targetNodes.reduceLeft(_ + "," + _))

  val machine = "$machine"
  def execCmds(cmd: Array[String], successMsg: String, failMsg: String, wait: Boolean = true) {
    val procs = targetNodes.par map { node =>
      val cmdline = cmd map { c => if (c.zip(machine).forall(p => p._1 == p._2)) node + c.substring(machine.length, c.length) else c }
      java.lang.Runtime.getRuntime.exec(cmdline)
    }
    if (wait) {
      val procResults = procs map { _.waitFor() }
      if (procResults.forall(_ == 0))
        info(successMsg)
      else
        error(failMsg + procResults.zip(targetNodes).filter(_._1 != 0).map(_._2).reduceLeft(_ + "," + _))
    }
  }

  val currDir = new java.io.File(".").getAbsolutePath.dropRight(2)
  val parDir = currDir.split('/').dropRight(1).reduceLeft(_ + "/" + _)

  if (!shutdown) {
    execCmds(Array("ssh", machine, "mkdir -p " + currDir + "/log"),
      "Created directories[OK]", "Directory creation failed for ")
    execCmds(Array("rsync", "--verbose", "--progress", "--stats", "--compress", "--rsh=ssh",
      "--recursive", "--times", "--perms", "--links", "--delete", "--exclude", "log",
      currDir, machine + ":" + parDir),
      "Completed rsync[OK]", "rsync failed for")

    val mupd8CP = Mupd8Main.getClass.getProtectionDomain.getCodeSource.getLocation.toString.
      split(':')(1).split('/').dropRight(1).reduceLeft(_ + "/" + _) + "/*"

    execCmds(Array("ssh", machine,
      "cd " + currDir + " && " +
        "nohup java " + config.javaSetting + " -cp " + mupd8CP + ":" + config.javaClassPath +
        " com.walmartlabs.mupd8.Mupd8Main -pidFile log/mupd8.pid " + args.reduceLeft(_ + " " + _) + " > log/run.log 2>&1"),
      "", "", false)
    info("Started Mupd8[OK]")
  } else {
    execCmds(Array("ssh", machine, "cat " + currDir + "/log/mupd8.pid | xargs kill"),
      "Completed shutdown[OK]", "shutdown failed for ")
  }
}

object Mupd8Main extends Logging {

  def main(args: Array[String]) {
    Thread.setDefaultUncaughtExceptionHandler(new Misc.TerminatingExceptionHandler())
    val syntax = Map("-s" -> (1, "Sys config file name"),
      "-a" -> (1, "App config file name"),
      "-d" -> (1, "Unified-config directory name"),
      "-sc" -> (1, "Mupd8 source class name"),
      "-sp" -> (1, "Mupd8 source class parameters separated by comma"),
      "-to" -> (1, "Stream to which data from the URI is sent"),
      "-threads" -> (1, "Optional number of execution threads, default is 5"),
      "-shutdown" -> (0, "Shut down the Mupd8 App"),
      "-pidFile" -> (1, "Optional PID filename"),
      // flag for turning on/off collection of statistics as a mupd8 app runs
      "-statistics" -> (1, "Collect statistics for monitoring?"),
      "-elastic" -> (1, "Computation is elastic in terms of number of hosts participating in a mupd8 application"))

    {
      val argMap = argParser(syntax, args)
      for {
        p <- argMap
        val shutdown = p.get("-shutdown") != None
        //            if shutdown || p.size == p.get("-threads").size + p.get("-pidFile").size + p.get("-a").size +
        //                                     p.get("-s").size + p.get("-d").size + syntax.size - 6
        if p.get("-s").size == p.get("-a").size
        if p.get("-s").size != p.get("-d").size
        threads <- excToOption(p.get("-threads").map(_.head.toInt).getOrElse(5))
        val launcher = p.get("-pidFile") == None
        /*
        COMMENT: Obtain the 'statistics' and 'elastic' flag from the configuration. These flags determine if monitoring and
                dyanmic load balancing is enabled, respectively.
        */
        val collectStatistics = if (p.get("-statistics") != None) { p.get("-statistics").get(0).equalsIgnoreCase("true") } else { false }
        val elastic = if (p.get("-elastic") != None) { p.get("-elastic").get(0).equalsIgnoreCase("true") } else { false }
      } yield {
        //Misc.configureLoggerFromXML("log4j.xml")
        val app = new AppStaticInfo(p.get("-d").map(_.head), p.get("-a").map(_.head), p.get("-s").map(_.head), !launcher, collectStatistics, elastic)
        if (launcher) {
          new MasterNode(args, app, shutdown)
        } else {
          p.get("-pidFile").map(x => writePID(x.head))
          val runtime = new AppRuntime(0, threads, app)
          if (runtime.ring != null) {
            if (app.sources.size > 0) {
              fetchFromSources(app, runtime)
            } else if (p.contains("-to") && p.contains("-sc")) {
              info("start source from cmdLine")
              runtime.startSource(p("-to").head, p("-sc").head, JavaConversions.seqAsJavaList(p("-sp").head.split(',')))
            }
          } else {
            error("Mupd8Main: no hash ring found, exiting...")
          }
          info("Goodbye")
        }
      }
    } getOrElse {
      error("Command Syntax error")
      error("Syntax is\n" + syntax.map(p => p._1 + " " + p._2._2 + "\n").reduceLeft(_ + _))
    }
  }

  def fetchFromSources(app: AppStaticInfo, runtime: AppRuntime): Unit = {
    val ssources = app.sources.asScala
    info("start source from sys cfg")
    object O {
      def unapply(a: Any): Option[org.json.simple.JSONObject] =
        if (a.isInstanceOf[org.json.simple.JSONObject])
          Some(a.asInstanceOf[org.json.simple.JSONObject])
        else None
    }
    ssources.foreach {
      case O(obj) => {
        if (isLocalHost(obj.get("host").asInstanceOf[String])) {
          val params = obj.get("parameters").asInstanceOf[java.util.List[String]]
          runtime.startSource(obj.get("performer").asInstanceOf[String], obj.get("source").asInstanceOf[String], params)
        }
      }
      case _ => { error("Wrong source format") }
    }
  }

}
