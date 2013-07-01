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
import scala.util.control.Breaks._
import scala.util.Random
import grizzled.slf4j.Logging
import java.net.InetAddress
import java.util.concurrent._
import java.io.ByteArrayOutputStream
import com.walmartlabs.mupd8.Misc._
import com.walmartlabs.mupd8.GT._
import com.walmartlabs.mupd8.miscM._
import com.walmartlabs.mupd8.Mupd8Type._
import com.walmartlabs.mupd8.application._

class AppRuntime(appID: Int,
                 poolsize: Int,
                 val appStatic: AppStaticInfo,
                 useNullPool: Boolean = false) extends Logging {
  val rand = new Random(System.currentTimeMillis())
  private val sourceThreads: mutable.ListBuffer[(String, List[java.lang.Thread])] = new mutable.ListBuffer
  val hostUpdateLock = new Object
  // Buffer for slates dest of which according to candidateRing is not current node anymore 
  // put definition here prevent null pointer exception when it is called at _ring init 
  val eventBufferForRingChange: ConcurrentLinkedQueue[PerformerPacket] = new ConcurrentLinkedQueue[PerformerPacket]();

  def initMapUpdatePool(poolsize: Int, runtime: AppRuntime, clusterFactory: (PerformerPacket => Unit) => MUCluster[PerformerPacket]): MapUpdatePool[PerformerPacket] =
    new MapUpdatePool[PerformerPacket](poolsize, runtime, clusterFactory)

  val msClient: MessageServerClient = if (appStatic.messageServerHost != None && appStatic.messageServerPort != None) {
    new MessageServerClient(appStatic.messageServerHost.get.asInstanceOf[String], appStatic.messageServerPort.get.asInstanceOf[Number].intValue(), 1000)
  } else {
    error("AppRuntime error: message server host name or port is empty")
    null
  }

  // start local server socket
  val localMessageServer = if (appStatic.messageServerPort != None) {
    new Thread(new LocalMessageServer(appStatic.messageServerPort.get.asInstanceOf[Number].intValue() + 1, this), "LocalMessageServer")
  } else {
    error("AppRuntime error: local message server port is None")
    null
  }
  if (localMessageServer != null) localMessageServer.start
  Thread.sleep(200) // Give local Message Server sometime to start

  val maxWorkerCount = 2 * Runtime.getRuntime.availableProcessors
  val slateURLserver = new HttpServer(appStatic.statusPort, maxWorkerCount, s => {
    val decoded = java.net.URLDecoder.decode(s, "UTF-8")
    val tok = decoded.split('/')
    if (tok(1) == "favicon.ico") None
    else if (tok(2) == "status") {
      excToOptionWithLog {
        Some(threadVect.map { p =>
          val perfPacket = p.perfPacket // Copy to local variable to avoid race condition
          if (perfPacket != null) appStatic.performers(perfPacket.pid).name + ":" + appStatic.performers(perfPacket.pid).mtype else "Waiting"
        }.
          groupBy(p => p).map(p => (p._1, p._2.size)).toList.
          sortWith(_._2 >= _._2).
          map(x => x._1 + (x._1.length to 50 map (_ => " ")).foldLeft("")(_ + _) + " " + x._2).foldLeft("")(_ + _ + "\n") +
          {
            val poolsizes = pool.threadDataPool.map(p => (p.queue.size, p.getSerialQueueSize())).sortWith { case ((a, b), (c, d)) => a + b < c + d }
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
        val key: (String, Key) = (tok(4), Key(tok(5).map(_.toByte).toArray))
        val poolKey = PerformerPacket.getKey(appStatic.performerName2ID(key._1), key._2)
        val dest = InetAddress.getByName(ring(poolKey)).getHostName
        if (appStatic.self.ip.compareTo(dest) == 0 || dest.compareTo("localhost") == 0 || dest.compareTo("127.0.0.1") == 0)
          getSlate(key)
        else {
          val slate = fetchURL("http://" + dest + ":" + (appStatic.statusPort + 300) + s)
          if (slate == None) {
            // TODO: send remove messageServer
            warn("Can't reach dest " + dest + "; going to report " + dest + " missing.")
            msClient.sendMessage(NodeRemoveMessage(Host(dest, appStatic.systemHosts(dest))))
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
  info("slateURLserver is up on port" + appStatic.statusPort)

  val pool = initMapUpdatePool(poolsize, this,
    action => new MUCluster[PerformerPacket](appStatic, appStatic.statusPort + 100,
                                             PerformerPacket(0, 0, Key(new Array[Byte](0)), Array(), "", this),
                                             () => { new Decoder(this) },
                                             action, msClient))

  /* generate Hash Ring */
  private var _ring: HashRing = null
  def ring = _ring // getter
  def ring_= (r: HashRing2): Unit = _ring = new HashRing(r.hash) // setter
  def ring_= (r: HashRing): Unit = _ring = new HashRing(r.hash)
  def ring_= (hash: IndexedSeq[String]): Unit = _ring = new HashRing(hash)

  // candidate ring and host list from message server
  var candidateRing: HashRing = null
  // (ip addresses, ip to hostname map)
  var candidateHostList: (IndexedSeq[String], immutable.Map[String, String]) = null

  // Try to Register host to message server and get updated hash ring from message server
  // even if hash ring is generated from system hosts already
  def trySendNodeJoinMessageToMessageServer(time: Int) {
    if (time <= 0) {
      error("Failed to send join message to message server, exiting...")
      System.exit(-1)
    } else {
      if (!msClient.sendMessage(NodeJoinMessage(appStatic.self))) {
        warn("Connecting to message server failed")
        Thread.sleep(500)
        trySendNodeJoinMessageToMessageServer(time - 500)
      }
    }
  }
  trySendNodeJoinMessageToMessageServer(10000) // try to send join message to message server for 10 sec
  info("Send node join message to message server")

  def waitHashRing(time: Int) {
    if (time <= 0) {
      error("Failed to update hash ring from message server, exiting...")
      System.exit(-1)
    } else {
      if (ring == null) {
        Thread.sleep(500)
        waitHashRing(time - 500)
      }
    }
  }
  waitHashRing(5000) // wait hash ring from message server for 5 sec
  info("Update ring from Message server")

  if (ring == null) {
    error("AppRuntime: No hash ring either from config file or message server")
    System.exit(-1)
  }

  val storeIo = if (useNullPool) new NullPool
                else new CassandraPool(appStatic.cassHosts,
                                       appStatic.cassPort,
                                       appStatic.cassKeySpace,
                                       p => (appStatic.performers(appStatic.performerName2ID(p)).cf).getOrElse(appStatic.cassColumnFamily),
                                       p => appStatic.performers(appStatic.performerName2ID(p)).ttl,
                                       appStatic.compressionCodec)

  val slateRAM: Long = Runtime.getRuntime.maxMemory / 5
  info("Memory available for use by Slate Cache is " + slateRAM + " bytes")
  val slateBuilders = appStatic.slateBuilderFactory.map(_.map(_.apply()))
  private val threadMap: Map[Long, TLS] = pool.threadDataPool.map(_.thread.getId -> new TLS(this))(breakOut)
  private val threadVect: Vector[TLS] = (threadMap map { _._2 })(breakOut)

  def getSlate(key: (String, Key)) = {
    val performerId = appStatic.performerName2ID(key._1)
    val host = ring(PerformerPacket.getKey(performerId, key._2))
    assert(host.compareTo(appStatic.self.ip) == 0 || host.compareTo("localhost") == 0 || host.compareTo("127.0.0.1") == 0)
    val future = new Later[SlateObject]
    getTLS(performerId, key._2).slateCache.waitForSlate(key, future.set(_), getUpdater(performerId, key._2), getSlateBuilder(performerId), false)
    val bytes = new ByteArrayOutputStream()
    getSlateBuilder(performerId).toBytes(future.get(), bytes)
    Option(bytes.toByteArray())
  }

  // We need a separate slate server that does not redirect to prevent deadlocks
  val slateServer = new HttpServer(appStatic.statusPort + 300, maxWorkerCount, s => {
    val tok = java.net.URLDecoder.decode(s, "UTF-8").split('/')
    getSlate((tok(4), Key(tok(5).map(_.toByte).toArray)))
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
          Key(data._key.getBytes()),
          data._value,
          stream,
          this))
      }
    }

    class SourceThread(sourceClassName: String,
                       sourceParams: java.util.List[String],
                       continuation: Mupd8DataPair => Unit) extends Runnable with Logging {
      override def run() = {
        breakable {
          val cls = Class.forName(sourceClassName)
          // For socket-type sources, construct a SourceReader by unlmited reattempts.
          val ins = cls.getConstructor(Class.forName("java.util.List")).newInstance(sourceParams).asInstanceOf[com.walmartlabs.mupd8.application.Mupd8Source]
          while (true) {
            try {
              if (ins.hasNext()) {
                val data = ins.getNextDataPair();
                continuation(data)
              } else {
                warn("SourceThread: hasNext returns false; stop this source thread")
                break() // end source thread at first no next returns
              }
            } catch {
              case e: Exception => error("SourceThread: exception during reads. Swallowed to continue next read.", e)
            } // catch everything to keep source running
          }
        }
      }
    }

    // TODO : Behavior for multiple performers is most probably wrong!!
    val threads: List[Thread] = for (
      perfID <- appStatic.performerName2ID.get(sourcePerformer).toList;
      if appStatic.performers(perfID).mtype == Source;
      edgeName <- appStatic.performers(perfID).pubs;
      destID <- appStatic.edgeName2IDs(edgeName);
      input <- excToOptionWithLog(new SourceThread(sourceClassName, sourceClassParams, initiateWork(destID, edgeName, _)))
    ) yield (new java.lang.Thread(input, "SourceReader:" + sourcePerformer + ":" + (sourceClassParams.asScala mkString ":")))

    if (!threads.isEmpty)
      sourceThreads += ((sourcePerformer, threads))
    else
      error("Unable to set up source for " + sourcePerformer + " server class " + sourceClassName + " with params: " + sourceClassParams)

    threads.foreach((t: Thread) => {
      t.setUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
        override def uncaughtException(t: Thread, e: Throwable): Unit = {
          error("SourceThread: hit exception... Swallowed to let main thread continue.", e)
        }
      })
      t.start() 
    })
    !threads.isEmpty
  }

  val writerThread = new Thread(run {
    while (true) {
      val startTime = java.lang.System.currentTimeMillis
      debug("writeThread: start to flush dirty slates")
      flushDirtySlateToCassandra()
      debug("writeThread: flush dirty slates is done")
      val exeuteTime = java.lang.System.currentTimeMillis - startTime
      // add around 2 seconds random sleep time
      val sleepTime = appStatic.cassWriteInterval * 1000 - exeuteTime + (if (rand.nextBoolean) -(rand.nextInt & 0x7ff) else rand.nextInt & 0x7ff)
      try {
    	if (sleepTime > 0) Thread.sleep(sleepTime)
      } catch {
        case e: Exception => warn("WriterThread: sleep hit exception", e)
      }
    }
  }, "writerThread")
  writerThread.start()

  // This step should be the last step in the initialization (it brings up the MapUpdatePool sockets)
  // to avoid exposing the JVM to external input before it is ready
  pool.init()

  def flushDirtySlateToCassandra() {
    if (threadVect != null) { // flush only when node is initted
      storeIo.initBatchWrite
      var dirtySlateList: List[((String, Key), SlateValue)] = List.empty
      threadVect foreach { tls => {
        val items: List[((String, Key), SlateValue)] = tls.slateCache.getDirtyItems
        dirtySlateList = dirtySlateList ++: items
        items.foreach (writeSlateToCassandra(_))
      }}
      if (storeIo.flushBatchWrite)
        dirtySlateList foreach ( _._2.dirty = false )
      storeIo.closeBatchWrite
    }
  }

  // During ring change process, flush dirty and dest changed slate into cassandra
  def flushFilteredDirtySlateToCassandra() {
    if (threadVect != null) { // flush only when node is initted
      info("flushFilteredDirtySlateToCassandra: starts to flush slates")
      storeIo.initBatchWrite
      var dirtySlateList: List[((String, Key), SlateValue)] = List.empty
      threadVect foreach { tls => {
        val items = tls.slateCache.getFilteredDirtyItems
        dirtySlateList ++:= items
        items.foreach (writeSlateToCassandra(_))
      }}
      if (storeIo.flushBatchWrite)
        dirtySlateList foreach ( _._2.dirty = false )
      storeIo.closeBatchWrite
      info("flushFilteredDirtySlateToCassandra: flush slates is done")
    }
  }

  // A utility method to write slate to cassandra.
  def writeSlateToCassandra(item: ((String, Key), SlateValue)) = {
    val key = item._1
    val slateVal = item._2
    val slateByteStream = new ByteArrayOutputStream()
    getSlateBuilder(appStatic.performerName2ID(key._1)).toBytes(slateVal.slate, slateByteStream)
    // TODO: .getBytes may not work the way you want it to!! Encoding issues!
    storeIo.batchWrite(key._1, key._2, slateByteStream.toByteArray())
  }

  // When ring change is done, flush events in buffer into local queue and process/dispatch them
  def flushSlatesInBufferToQueue() {
	info("Putting slates in buffer to pool")
	// Check buf size before call foreach on it
	// since foreach on empty buf sometime causes exception
    if (!eventBufferForRingChange.isEmpty) {
      eventBufferForRingChange.foreach(e => pool.putLocal(e.getKey, e))
      eventBufferForRingChange.clear
    }
  }
  
  // Wait current running performer jobs done for prepare ring change  
  def waitPerformerJobsDone() {
    if (threadVect != null)
      while (pool.threadDataPool.filter(t => t.started && !t.noticedCandidateRing).size > 0) {
        // TODO: come with a better wait/notify solution
        Thread.sleep(500)
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