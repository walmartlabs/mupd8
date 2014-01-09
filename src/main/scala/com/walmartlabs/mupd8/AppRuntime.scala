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
import com.walmartlabs.mupd8.Misc._
import com.walmartlabs.mupd8.Mupd8Type._
import com.walmartlabs.mupd8.application._
import scala.annotation.tailrec
import scala.util.Try
import scala.util.Failure
import scala.util.Success
import scala.util.Success

class AppRuntime(appID: Int,
                 poolsize: Int,
                 val appStatic: AppStaticInfo,
                 useNullPool: Boolean = false) extends Logging {

  /*
   * Startup sequence
   * 1. init storeIO
   * 2. start local message server
   * 3. get host name from message server, including starting message server
   * 4. create mapupdater pool
   * 5. join cluster
   */
  // 1. init storeIO
  val sysStartTime = System.currentTimeMillis()
  val rand = new Random(System.currentTimeMillis())
  val storeIO = if (useNullPool) new NullPool
                else new CassandraPool(appStatic.cassHosts,
                                       appStatic.cassPort,
                                       appStatic.cassKeySpace,
                                       p => (appStatic.performers(appStatic.performerName2ID(p)).cf).getOrElse(appStatic.cassColumnFamily),
                                       p => appStatic.performers(appStatic.performerName2ID(p)).ttl,
                                       appStatic.compressionCodec)
  var startedSources: Map[String, Host] = Map.empty // (source name -> source host machine)
  // start source lock and list of started sources on this node
  var startedSourcesOnThisNode: Set[String] = Set.empty
  val startSourceLock = new Object()

  var messageServerHost: Host = null
  val messageServerPort: Int = appStatic.messageServerPortFromConfig
  var startedMessageServer = false

  // 2. start local server socket
  val localMessageServer = new Thread(new LocalMessageServer(messageServerPort + 1, this), "LocalMessageServer")
  localMessageServer.start
  Thread.sleep(200) // Give local Message Server sometime to start

  // 3. get hostname from message server
  info("Connect to message server " + (messageServerHost, messageServerPort) + " to decide hostname")
  // if this node is specified in config as message server, try less times
  val firstCheckIpRlt: Option[Host] = getLocalHostName(if (Misc.isLocalHost(appStatic.messageServerHostFromConfig)) 5 else 20)
  val self: Host = firstCheckIpRlt match {
    case None =>
      if (Misc.isLocalHost(appStatic.messageServerHostFromConfig)) {
        // if message server is set to this node in config file, restart message server on this node
        info("Start message server according to config file")
        startMessageServer()
        // write itself into db store as message server
        storeIO.writeColumn(appStatic.cassColumnFamily, CassandraPool.PRIMARY_ROWKEY, CassandraPool.MESSAGE_SERVER, InetAddress.getByName(appStatic.messageServerHostFromConfig).getHostAddress())
        // clear started source reader in db store
        storeIO.writeColumn(appStatic.cassColumnFamily, CassandraPool.PRIMARY_ROWKEY, CassandraPool.STARTED_SOURCES, "")
        Thread.sleep(5000) // yield cpu to startMessageServer and cassandra writing
        getLocalHostName(5) match {
          case None => error("Check local host name failed, exit..."); System.exit(-1); null
          case Some(host) => host
        }
      } else {
        error("Check local host name failed, exit..."); System.exit(-1); null
      }
    case Some(host) => host
  }
  info("Host id is " + self)
  // init msClient
  var msClient: MessageServerClient = new MessageServerClient(this, 1000)

  // 4. mapupdater pool is needed by ring update
  // pool depends on mucluster, mucluster depends on msClient
  val pool = initMapUpdatePool(poolsize, this,
    action => new MUCluster[PerformerPacket](self, appStatic.statusPort + 100,
                                             PerformerPacket(0, 0, Key(new Array[Byte](0)), Array(), "", this),
                                             () => { new Decoder(this) },
                                             action, this))
  val slateRAM: Long = Runtime.getRuntime.maxMemory / 5
  info("Memory available for use by Slate Cache is " + slateRAM + " bytes")
  val slateBuilders = appStatic.slateBuilderFactory.map(_.map(_.apply()))

  // TLS depends on storeIo
  private val threadMap: Map[Long, TLS] = pool.threadDataPool.map(_.thread.getId -> new TLS(this))(breakOut)
  private val threadVect: Vector[TLS] = (threadMap map { _._2 })(breakOut)

  // 5. ask to join cluster and update hash ring
  private var _ring: HashRing = null
  def ring = _ring // getter
  def ring_= (r: HashRing): Unit = {_ring = HashRing.initFromRing(r)} // setter
  // candidate ring and host list from message server
  var candidateRing: HashRing = null
  private val sourceThreads: collection.mutable.ListBuffer[(String, List[java.lang.Thread])] = new collection.mutable.ListBuffer
  val hostUpdateLock = new Object
  // Buffer for slates dest of which according to candidateRing is not current node anymore
  // put definition here prevent null pointer exception when it is called at _ring init
  val eventBufferForRingChange: ConcurrentLinkedQueue[PerformerPacket] = new ConcurrentLinkedQueue[PerformerPacket]();

  trySendNodeJoinMessageToMessageServer(10000)  // try to send join message to message server for 10 sec
  info("Send node join message to message server")
  waitHashRing(60000) // wait hash ring from message server for 5 sec
  info("Update ring from Message server")

  if (ring == null) {
    error("AppRuntime: No hash ring either from c¬onfig file or message server")
    System.exit(-1)
  }

  // Start heart beat message server
  val heartBeat = new HeartBeat(this)
  heartBeat.start()

  // Read previous message server settings
  // check message server, (message_server_host, port), from data store
  def setMessageServerFromDataStore() {
    @tailrec
    def fetchMessageServerFromDataStore(): Unit = {
      info("Fetching message server address from db store")
      Try(storeIO.fetchStringValueColumn(appStatic.cassColumnFamily, CassandraPool.PRIMARY_ROWKEY, CassandraPool.MESSAGE_SERVER)) match {
        case Failure(ex) =>
          error("fetchMessageServerFromDataStore: failed to fetch message server config from data store")
          if (Misc.isLocalHost(appStatic.messageServerHostFromConfig)) {
            // cluster first time start, no message server in data store
            // then start message server, write message server config into data store
            startMessageServer()
            // write ip address into db store as message server
            storeIO.writeColumn(appStatic.cassColumnFamily, CassandraPool.PRIMARY_ROWKEY, CassandraPool.MESSAGE_SERVER, InetAddress.getByName(appStatic.messageServerHostFromConfig).getHostAddress())
            // clear started source reader in db store
            storeIO.writeColumn(appStatic.cassColumnFamily, CassandraPool.PRIMARY_ROWKEY, CassandraPool.STARTED_SOURCES, "")
            messageServerHost = Host(InetAddress.getByName(appStatic.messageServerHostFromConfig).getHostAddress(), appStatic.messageServerHostFromConfig)
            info("Set message server from config - " + (messageServerHost, messageServerPort))
          } else {
            if (System.currentTimeMillis() - sysStartTime > appStatic.startupTimeout * 1000) {
              error("fetchMessageServer timeout, exiting...")
              System.exit(-1)
            } else {
              // sleep and retry
              Thread.sleep(500)
              fetchMessageServerFromDataStore()
            }
          }
        case Success(msgserver) =>
          info("Message server config from data store: " + msgserver)
          messageServerHost = Host(msgserver, InetAddress.getByName(msgserver).getHostName())
      }
    }

    fetchMessageServerFromDataStore()
  }

  def startMessageServer() {
    startedMessageServer = true

    info("Start message server at " + appStatic.messageServerPortFromConfig)
    // save all listed sources
    val allSourcesFromConfig: Map[String, Source] = (for {
      source <- appStatic.sources;
      sourceName = source.get("name").asInstanceOf[String];
      sourcePerformer = source.get("performer").asInstanceOf[String];
      sourceClass = source.get("source").asInstanceOf[String];
      params = source.get("parameters").asInstanceOf[java.util.List[String]].asScala.toList
    } yield (sourceName -> new Source(sourceName, sourceClass, sourcePerformer, params))).toMap

    // start server thread
    val server = new MessageServer(this, Integer.valueOf(appStatic.messageServerPortFromConfig), allSourcesFromConfig)
    server.start

    Runtime.getRuntime().addShutdownHook(new Thread { override def run = server.shutdown() })

    info("MessageServer started")
  }

  // check ip and host by connecting to message server
  // if message server is not responsive, retrieve message server config from data store and retry
  def getLocalHostName(retryTimes: Int): Option[Host] = {
    def _getLocalHostName(retryCount: Int): Option[Host] = {
      if (retryCount >= retryTimes) None
      else {
        setMessageServerFromDataStore()
        new MessageServerClient(this, 1000).checkIP() match {
          case None => Thread.sleep(5000); warn("getHostName again"); _getLocalHostName(retryCount + 1)
          case Some(host) => Some(host)
        }
      }
    }

    _getLocalHostName(0)
  }

  def initMapUpdatePool(poolsize: Int, runtime: AppRuntime, clusterFactory: (PerformerPacket => Unit) => MUCluster[PerformerPacket]): MapUpdatePool[PerformerPacket] =
    new MapUpdatePool[PerformerPacket](poolsize, runtime, clusterFactory)

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
              "\nPending IOs: " + storeIO.pendingCount +
              "\n"
          }).get.getBytes
      }
    } else if (tok(2) == "slate") {
      if (tok.length != 6) {
        None
      } else if (ring == null) {
        info("Query request before node is up")
        None
      } else {
        val key: (String, Key) = (tok(4), Key(tok(5).map(_.toByte).toArray))
        val poolKey = PerformerPacket.getKey(appStatic.performerName2ID(key._1), key._2)
        val dest = ring(poolKey)
        if (self.ip.compareTo(dest) == 0 || dest.compareTo("localhost") == 0 || dest.compareTo("127.0.0.1") == 0)
          getSlate(key)
        else {
          val slate = fetchURL("http://" + dest + ":" + (appStatic.statusPort + 300) + s)
          slate match {
            case None =>
              warn("Can't reach dest(" + dest + "); going to report " + dest + " fails.")
              val performerId = appStatic.performerName2ID(key._1)
              val host = ring(PerformerPacket.getKey(performerId, key._2))
              val future = new Later[SlateObject]
              storeIO.fetchSlates(key._1, key._2, p => future.set(p))
              val bytes = new ByteArrayOutputStream()
              getSlateBuilder(performerId).toBytes(future.get(), bytes)
              Some(bytes.toByteArray())
            case Some(s) => Some(s)
          }
        }
      }
    } else if (tok(2) == "ring") {
      if (ring == null) Some("null\n".getBytes)
      else Some((ring.toString + "\n").getBytes)
    } else if (tok(2) == "sources") {
      // load started sources from db store
      Try(storeIO.fetchStringValueColumn(appStatic.cassColumnFamily, CassandraPool.PRIMARY_ROWKEY, CassandraPool.STARTED_SOURCES)) match {
        case Failure(ex) => Some("No source reader started\n".getBytes)
        case Success(str) =>
          // load started sources from db store
          // one pair format: key + 0x1d + value + \n
          if (str.length == 0) Some("No source reader started\n".getBytes)
          else {
            val rlt = str.replaceAll(0x1d.toChar.toString, " -> ").replaceAll("\n", "; ").trim()
            Some(rlt.substring(0, rlt.length - 1).concat("\n").getBytes)
          }
      }
    } else if (tok(2) == "messageserver") {
      Some((messageServerHost.toString + "\n").getBytes())
    } else {
      // This section currently handle varnish probe -- /mupd8/config/app
      // TODO: how to handle other requests?
      Some("{}".getBytes)
    }
  })
  slateURLserver.start
  info("slateURLserver is up on port: " + appStatic.statusPort)

  // Try to Register host to message server and get updated hash ring from message server
  // even if hash ring is generated from system hosts already
  def trySendNodeJoinMessageToMessageServer(time: Int) {
    if (time <= 0) {
      error("Failed to send join message to message server, exiting...")
      System.exit(-1)
    } else {
      if (!msClient.sendMessage(NodeChangeMessage(Set(self), Set.empty))) {
        warn("Connecting to message server failed")
        Thread.sleep(500)
        trySendNodeJoinMessageToMessageServer(time - 500)
      }
    }
  }

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

  def getSlate(key: (String, Key)) = {
    val performerId = appStatic.performerName2ID(key._1)
    val host = ring(PerformerPacket.getKey(performerId, key._2))
    assert(host.compareTo(self.ip) == 0 || host.compareTo("localhost") == 0 || host.compareTo("127.0.0.1") == 0)
    val future = new Later[SlateObject]
    getTLS(performerId, key._2).slateCache.waitForSlate(key, future.set(_), getUpdater(performerId, key._2), getSlateBuilder(performerId), false)
    val bytes = new ByteArrayOutputStream()
    getSlateBuilder(performerId).toBytes(future.get(), bytes)
    Option(bytes.toByteArray())
  }

  // We need a separate slate server that does not redirect to prevent deadlocks
  val slateServer = new HttpServer(appStatic.statusPort + 300, maxWorkerCount, s => {
    info("slateServer: received " + s)
    val tok = java.net.URLDecoder.decode(s, "UTF-8").split('/')
    getSlate((tok(4), Key(tok(5).map(_.toByte).toArray)))
  })
  slateServer.start

  // name set of sources started on this node
  def startSource(sourceName: String): Boolean = {
    startSourceLock.synchronized {
      if (!startedSourcesOnThisNode.contains(sourceName)) {
        val source = appStatic.sources.asScala.toList.find(s => sourceName.compareTo(s.get("name").asInstanceOf[String]) == 0)
        source match {
          case Some(source: org.json.simple.JSONObject) =>
            val sourcePerformer = source.get("performer").asInstanceOf[String]
            val sourceClass = source.get("source").asInstanceOf[String]
            val params = source.get("parameters").asInstanceOf[java.util.List[String]]
            val rt = startSource(sourceName, sourcePerformer, sourceClass, params)
            startedSourcesOnThisNode = startedSourcesOnThisNode + sourceName
            rt
          case _ =>
            error("Source(" + sourceName + ") doesn't exist")
            false
        }
      } else {
        info("StartSource: " + sourceName + " is already started on this node")
        true
      }
    }
  }

  def startSource(sourceName: String, sourcePerformer: String, sourceClassName: String, sourceClassParams: java.util.List[String]): Boolean = {
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
        val packet = PerformerPacket(SOURCE_PRIORITY,
          performerID,
          Key(data._key.getBytes()),
          data._value,
          stream,
          this)
        pool.putSource(packet)
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
      sourceThreads += (sourcePerformer -> threads)
    else
      error("Unable to set up source for " + sourcePerformer + " server class " + sourceClassName + " with params: " + sourceClassParams)

    threads.foreach((t: Thread) => {
      t.setUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
        override def uncaughtException(t: Thread, e: Throwable): Unit = {
          error("SourceThread: hit exception... Swallowed to let main thread continue.", e)
        }
      })
      t.start()
      // record started source
      startedSources = startedSources + (sourceName -> self)
    })
    !threads.isEmpty
  }

  val writerThread = new Thread(run {
    while (true) {
      val startTime = java.lang.System.currentTimeMillis
      trace("writeThread: start to flush dirty slates")
      flushDirtySlateToCassandra()
      trace("writeThread: flush dirty slates is done")
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

  def flushDirtySlateToCassandra() {
    if (threadVect != null) { // flush only when node is initted
      storeIO.initBatchWrite
      var dirtySlateList: List[((String, Key), SlateValue)] = List.empty
      threadVect foreach { tls => {
        val items: List[((String, Key), SlateValue)] = tls.slateCache.getDirtyItems
        items.foreach { i => {
          if (writeSlateToCassandra(i)) {
            dirtySlateList = i :: dirtySlateList
          } else {
            debug("flushDirtySlateToCassandra: forcing partial flush")

            if (storeIO.flushBatchWrite)
              dirtySlateList foreach ( _._2.dirty = false )
            else
              error("flushDirtySlateToCassandra: flush failed; slates left dirty for retry")

            storeIO.closeBatchWrite
            dirtySlateList = List.empty
            storeIO.initBatchWrite

            if (writeSlateToCassandra(i)) {
              dirtySlateList = i :: dirtySlateList
            } else {
              error("flushDirtySlateToCassandra: could not write slate "+i._1+" "+i._2+" into new Cassandra operation; is Misc.SLATE_CAPACITY too large?")
            }
          }
        }}
      }}
      if (storeIO.flushBatchWrite)
        dirtySlateList foreach ( _._2.dirty = false )
      else
        error("flushDirtySlateToCassandra: flush failed, slates left dirty for retry")

      storeIO.closeBatchWrite
    }
  }

  // During ring change process, flush dirty and dest changed slate into cassandra
  def flushFilteredDirtySlateToCassandra() {
    if (threadVect != null) { // flush only when node is initted
      info("flushFilteredDirtySlateToCassandra: starts to flush slates")
      storeIO.initBatchWrite
      var dirtySlateList: List[((String, Key), SlateValue)] = List.empty
      threadVect foreach { tls => {
        val items = tls.slateCache.getFilteredDirtyItems
        items.foreach { i => {
          if (writeSlateToCassandra(i)) {
            dirtySlateList = i :: dirtySlateList
          } else {
            debug("flushFilteredDirtySlateToCassandra: forcing partial flush")

            if (storeIO.flushBatchWrite)
              dirtySlateList foreach ( _._2.dirty = false )
            else
              error("flushFilteredDirtySlateToCassandra: flush failed; slates left dirty for retry")

            storeIO.closeBatchWrite
            dirtySlateList = List.empty
            storeIO.initBatchWrite

            if (writeSlateToCassandra(i)) {
              dirtySlateList = i :: dirtySlateList
            } else {
              error("flushFilteredDirtySlateToCassandra: could not write slate "+i._1+" "+i._2+" into new Cassandra operation; is Misc.SLATE_CAPACITY too large?")
            }
          }
        }}
      }}
      if (storeIO.flushBatchWrite)
        dirtySlateList foreach ( _._2.dirty = false )
      else
        error("flushFilteredDirtySlateToCassandra: flush failed; slates left dirty for retry")

      storeIO.closeBatchWrite
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
    storeIO.batchWrite(key._1, key._2, slateByteStream.toByteArray())
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
  def getTLS = threadMap(Thread.currentThread().getId)

  def getMapper(pid: Int) = getTLS.objects(pid).get.asInstanceOf[binary.Mapper]

  def getUpdater(pid: Int) = getTLS.objects(pid).get.asInstanceOf[binary.SlateUpdater]
  def getUpdater(pid: Int, key: Key) = getTLS(pid, key).objects(pid).get.asInstanceOf[binary.SlateUpdater]
  def getUnifiedUpdater(pid: Int) = getTLS.objects(pid).get.asInstanceOf[binary.UnifiedUpdater]

  // This hash function must be the same as the MapUpdatePool and different from SlateCache
  def getTLS(pid: Int, key: Key) =
    threadVect(pool.getPreferredPoolIndex(PerformerPacket.getKey(pid, key)))

  // find next message server
  def nextMessageServer(): Option[String] = {
    // check with head of ip list
    def _checkNode(nodes: IndexedSeq[String]): Option[String] = {
      if (nodes.isEmpty) {
        None
      } else {
        val next = nodes.head
        if (next.compareTo(messageServerHost.ip) == 0) _checkNode(nodes.tail)
        else {
          val lmsclient = new LocalMessageServerClient(next, messageServerPort + 1)
          if (!lmsclient.sendMessage(ToBeNextMessageSeverMessage(self))) {
            _checkNode(nodes.tail)
          }
          Some(next)
        }
      }
    }

    _checkNode(ring.ips);
  }

}
