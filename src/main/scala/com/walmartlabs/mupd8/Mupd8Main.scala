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

import scala.collection.immutable
import scala.collection.mutable
import scala.collection.breakOut
import scala.collection.JavaConverters._
import scala.collection.JavaConversions._
import scala.util.Sorting
import scala.util.parsing.json.JSON
import java.util.concurrent._
import java.util.ArrayList
import java.util.Arrays
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
import com.walmartlabs.mupd8.application.binary
import com.walmartlabs.mupd8.compression.CompressionFactory
import com.walmartlabs.mupd8.compression.CompressionService
import com.walmartlabs.mupd8.Misc._
import com.walmartlabs.mupd8.application._
import com.walmartlabs.mupd8.network.common.Decoder.DecodingState
import com.walmartlabs.mupd8.network.client._
import com.walmartlabs.mupd8.network.server._
import com.walmartlabs.mupd8.network.common._
import grizzled.slf4j.Logging
import com.walmartlabs.mupd8.network.common.Decoder.DecodingState._
import java.nio.ByteBuffer
import java.io.ByteArrayOutputStream
import GT._
import scala.actors.Actor
import Mupd8Type._

case class Host(ip: String, hostname: String)

class MapUpdatePool[T <: MapUpdateClass[T]](val poolsize: Int, appRun: AppRuntime, clusterFactory: (T => Unit) => MUCluster[T]) extends Logging {
  case class innerCompare(job: T, key: PerformerPacketKey) extends Comparable[innerCompare] {
    override def compareTo(other: innerCompare) = job.compareTo(other.job)
  }
  val ring = appRun.ring

  class ThreadData(val me: Int) {
    val queue = new PriorityBlockingQueue[innerCompare]
    private[MapUpdatePool] var keyInUse: Any = null
    private[MapUpdatePool] var keyQueue = new mutable.Queue[Runnable]
    private[MapUpdatePool] val keyLock = new scala.concurrent.Lock
    // flags used in ring change
    // started: a job from queue is started
    var started = false;
    // noticedCandidateRing: a candidate ring from message server is set
    // before job from queue is started
    var noticedCandidateRing = false;

    val thread = new Thread(run {
      while (true) {
        val item = queue.take()
        started = true
        noticedCandidateRing = (appRun.candidateRing != null)
        if (item.key == null) {
          item.job.run() // This is a mapper job
        } else {
          val (i1, i2) = getPoolIndices(item.key)
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
              work map { w => w.run() }
              jobCount += 1
              work != None
            }) {}
            if (currentlyHot) {
              Thread.currentThread.setPriority(Thread.NORM_PRIORITY)
            }
          }
        }
        // TODO: come with a better wait/notify solution
        //if (ring2 != null && !noticedRing2) notify();
        started = false
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

  val threadDataPool = 0 until poolsize map { new ThreadData(_) }
  private val rand = new java.util.Random(System.currentTimeMillis)
  val cluster = clusterFactory(p => putLocal(p.getKey, p))
  def init() { cluster.init() }

  def mod(i: Int) = if (i < 0) -i else i

  private val HASH_CONSTANT = 17
  // Get queues in queue for key
  private def getPoolIndices(key: Any) = {
    val fullhash = key.hashCode()
    val hash = fullhash / HASH_CONSTANT //cluster.hosts.size
    val i1 = hash % threadDataPool.size
    val i2 = (hash / threadDataPool.size) % (threadDataPool.size - 1)
    val (m1, m2) = (mod(i1), mod(i2))
    (m1, if (m2 < m1) m2 else m2 + 1)
  }

  def getPreferredPoolIndex(key: Any) = {
    val fullhash = key.hashCode()
    val hash = fullhash / HASH_CONSTANT //cluster.hosts.size
    mod(hash % threadDataPool.size)
  }

  private def lock(i1: Int, i2: Int) {
    val (k1, k2) = if (i1 < i2) (i1, i2) else (i2, i1)
    threadDataPool(k1).keyLock.acquire()
    if (k1 != k2) threadDataPool(k2).keyLock.acquire()
  }

  private def unlock(i1: Int, i2: Int) {
    val (k1, k2) = if (i1 < i2) (i1, i2) else (i2, i1)
    threadDataPool(k2).keyLock.release()
    if (k1 != k2) threadDataPool(k1).keyLock.release()
  }

  // This method should only be called after acquiring the (i1,i2) locks
  private def attemptQueue(job: Runnable with Comparable[T], key: Any, i1: Int, i2: Int): Boolean = {
    val (p1, p2) = (threadDataPool(i1), threadDataPool(i2))

    val b1 = if (p1.keyInUse != null) p1.keyInUse == key else false
    val b2 = if (p2.keyInUse != null) p2.keyInUse == key else false
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
    val a = rand.nextInt(threadDataPool.size) //TODO: Do we need to serialize this call?
    val sa = threadDataPool(a).keyQueue.size + threadDataPool(a).queue.size()

    val destination =
      if (sa > 1) {
        val temp = rand.nextInt(threadDataPool.size - 1)
        val b = if (temp < a) temp else temp + 1
        if (threadDataPool(b).keyQueue.size + threadDataPool(b).queue.size < sa) b else a
      } else a

    threadDataPool(destination).queue.put(innerCompare(x, null))
  }

  // Put source into queue
  def putSource(x: T) {
    var a = 0
    var sa = 0
    while ({
      a = rand.nextInt(threadDataPool.size) //TODO: Do we need to serialize this call?
      sa = threadDataPool(a).keyQueue.size + threadDataPool(a).queue.size()
      sa > 50
    }) {
      java.lang.Thread.sleep((sa - 50L) * (sa - 50L) / 25 min 1000)
    }

    threadDataPool(a).queue.put(innerCompare(x, null))
  }

  def putLocal(key: PerformerPacketKey, x: T) { // TODO : Fix key : Any??
    val (i1, i2) = getPoolIndices(key)
    lock(i1, i2)

    if (!attemptQueue(x, key, i1, i2)) {
      // TODO: HOT conductor check not accurate, use time stamps
      val (p1, p2) = (threadDataPool(i1), threadDataPool(i2))
      val dest = if (p1.keyQueue.size + p1.queue.size > 1.3 * (p2.keyQueue.size + p2.queue.size)) p2 else p1
      dest.queue.put(innerCompare(x, key))
    }

    unlock(i1, i2)
  }

  def put(key: PerformerPacketKey, x: T) {
    val dest = appRun.ring(key)
    if (appRun.appStatic.self.ip.compareTo(dest) == 0
        ||
        // during ring chagne process, if dest is going to be removed from cluster
        (appRun.candidateHostList != null && !appRun.candidateHostList._2.contains(dest)))
      putLocal(key, x)
    else
      cluster.send(dest, x)
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

class TLS(val appRun: AppRuntime) extends binary.PerformerUtilities with Logging {
  val objects = appRun.appStatic.performerFactory.map(_.map(_.apply()))
  // val slateCache = new SlateCache(appRun.storeIo, appRun.slateRAM / appRun.pool.poolsize)
  val slateCache = new SlateCache(appRun.storeIo, appRun.appStatic.slateCacheCount, this)
  val queue = new PriorityBlockingQueue[Runnable]
  var perfPacket: PerformerPacket = null
  var startTime: Long = 0

  val unifiedUpdaters: Set[Int] =
    (for (
      (oo, i) <- objects.zipWithIndex;
      o <- oo;
      if excToOption(o.asInstanceOf[binary.UnifiedUpdater]) != None
    ) yield i)(breakOut)

  override def publish(stream: String, key: Array[Byte], event: Array[Byte]) {
    trace("TLS::Publish: Publishing to " + stream + " Key " + str(key) + " event " + str(event))
    appRun.appStatic.edgeName2IDs.get(stream).map(_.foreach(
      pid => {
        trace("TLS::publish: Publishing to " + appRun.appStatic.performers(pid).name)
        val packet = PerformerPacket(NORMAL_PRIORITY, pid, Key(key), event, stream, appRun)
        if (appRun.appStatic.performers(pid).mtype == Mapper)
          appRun.pool.put(packet)  // publish to mapper?
        else
          appRun.pool.put(packet.getKey, packet)
      })).getOrElse(error("publish: Bad Stream name" + stream))
  }

  //  import com.walmartlabs.mupd8.application.SlateSizeException
  @throws(classOf[SlateSizeException])
  override def replaceSlate(slate: SlateObject) {
    // TODO: Optimize replace slate to avoid hash table look ups
    val name = appRun.appStatic.performers(perfPacket.pid).name
    assert(appRun.appStatic.performers(perfPacket.pid).mtype == Updater)
    val cache = appRun.getTLS(perfPacket.pid, perfPacket.slateKey).slateCache
    trace("replaceSlate " + appRun.appStatic.performers(perfPacket.pid).name + "/" + perfPacket.slateKey + " Oldslate " + (cache.getSlate((name,perfPacket.slateKey)).get).toString() + " Newslate " + slate.toString())
    cache.put((name, perfPacket.slateKey), slate)
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
      "-pidFile" -> (1, "Optional PID filename"))

    {
      val argMap = argParser(syntax, args)
      for {
        p <- argMap
        if p.get("-s").size == p.get("-a").size
        if p.get("-s").size != p.get("-d").size
        threads <- excToOption(p.get("-threads").map(_.head.toInt).getOrElse(5))
      } yield {
        val shutdown = p.get("-shutdown") != None
        val app = new AppStaticInfo(p.get("-d").map(_.head), p.get("-a").map(_.head), p.get("-s").map(_.head))
        p.get("-pidFile") match {
          case None => writePID("mupd8.log")
          case Some(x) => writePID(x.head)
        }
        val runtime = new AppRuntime(0, threads, app)
        if (runtime.ring != null) {
          if (app.sources.size > 0) {
            startSources(app, runtime)
          } else if (p.contains("-to") && p.contains("-sc")) {
            info("start source from cmdLine")
            runtime.startSource("cmdLineSource", p("-to").head, p("-sc").head, seqAsJavaList(p("-sp").head.split(',')))
          }
        } else {
          error("Mupd8Main: no hash ring found, exiting...")
        }
        info("Init is done")
        //        }
      }
    } getOrElse {
      error("Command Syntax error")
      error("Syntax is\n" + syntax.map(p => p._1 + " " + p._2._2 + "\n").reduceLeft(_ + _))
    }
  }

  def startSources(app: AppStaticInfo, runtime: AppRuntime) {
    class AskPermit(sourceName: String) extends Actor {
      def act() {
        if (!app.messageServerHost.isDefined || !app.messageServerPort.isDefined) {
          error("startSource: Host or Port is None, " + (app.messageServerHost, app.messageServerPort))
          exit()
        }

        val client: MessageServerClient = new MessageServerClient(app.messageServerHost.get.asInstanceOf[String], app.messageServerPort.get.asInstanceOf[Number].intValue(), 1000)
        client.sendMessage(AskPermitToStartSourceMessage(sourceName, app.self))
      }
    }

    val ssources = app.sources.asScala
    info("start source from sys cfg")
    ssources.foreach { source =>
      if (isLocalHost(source.get("host").asInstanceOf[String])) {
        val sourceName = source.get("name").asInstanceOf[String]
        val askPermit = new AskPermit(sourceName)
        askPermit.start
      }
    }
  }

}
