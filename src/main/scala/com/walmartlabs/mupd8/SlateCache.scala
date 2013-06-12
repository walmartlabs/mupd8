package com.walmartlabs.mupd8

import com.walmartlabs.mupd8.miscM._
import com.walmartlabs.mupd8.GT._
import com.walmartlabs.mupd8.application.binary
import grizzled.slf4j.Logging
import scala.collection._

class SlateValue(val slate: SlateObject, val keyRef: Node[String], var dirty: Boolean = true)

class SlateCache(val io: IoPool, val usageLimit: Long, val tls: TLS) extends Logging {
  println("Creating new SlateCache with IoPool " + io + " and usageLimit " + usageLimit);
  private val lock = new scala.concurrent.Lock
  private val table = new mutable.HashMap[String, SlateValue]()
  private val lru = new DLList[String]

  private var currentUsage: Long = 0
  private val objOverhead: Long = 32

  def buildKey(name: String, key: Key) = name + "~~~" + str(key.value)

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
        io.fetch(akey._1, akey._2, p => action(safePut(akey, p.map {slateFromBytes}.getOrElse{updater.getDefaultSlate()})))
      } else {
        io.fetch(akey._1, akey._2, p => action(p.map {slateFromBytes}.getOrElse{updater.getDefaultSlate()}))
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

  // Used only by replaceSlate
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

  def getDirtyItems(): List[(String, SlateValue)] = {
    lock.acquire()
    val retVal = table.filter(_._2.dirty).toList
    lock.release()
    retVal
  }

  // filter out (string, slatevalue) list which needs to be flushed into cassandra during ring change
  def getFilteredDirtyItems(): List[(String, SlateValue)] = {
    lock.acquire
    val retVal = table.filter(x => if (!x._2.dirty) false
                                   else if (tls.appRun.candidateRing == null) false
                                   else {
                                     // decompose key to build performerpacket key
                                     val key1 = x._1.take(x._1.indexOf("~~~"))
                                     val key2 = x._1.drop(key1.length + 3)
                                     tls.appRun.candidateRing(PerformerPacket.getKey(tls.appRun.appStatic.performerName2ID(key1), Key(key2.getBytes))) != tls.appRun.appStatic.self
                                   }).toList
    lock.release
    retVal
  }

  def getAllItems() = {
    lock.acquire()
    val retVal = table.toList
    lock.release()
    retVal
  }

}
