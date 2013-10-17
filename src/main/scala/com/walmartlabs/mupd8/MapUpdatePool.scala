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

import java.util.concurrent.PriorityBlockingQueue
import grizzled.slf4j.Logging
import scala.collection.immutable
import scala.collection.mutable
import com.walmartlabs.mupd8.Misc._

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
    // at startup appRun.ring could be null
    if (appRun.ring != null) {
      val destip = appRun.ring(key)
      if (appRun.self.ip.compareTo(destip) == 0
        ||
        // during ring chagne process, if dest is going to be removed from cluster
        (appRun.candidateRing != null && !appRun.candidateRing.ips.contains(destip)))
        putLocal(key, x)
      else
        cluster.send(Host(destip, appRun.ring.ipHostMap(destip)), x)
    }
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
