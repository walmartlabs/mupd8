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

import java.io.InputStream
import java.net.InetAddress
import java.net.Inet4Address
import java.net.NetworkInterface
import java.util.concurrent.Semaphore
import java.util.Arrays
import scala.collection.JavaConverters._
import grizzled.slf4j.Logging
import org.apache.http.client.methods.HttpGet
import org.apache.http.util.EntityUtils
import org.apache.http.impl.client.HttpClients

object Misc extends Logging {
  val HTTPCLIENT = HttpClients.createDefault();

  val SLATE_CAPACITY = 1048576 // 1M size
  val COMPRESSED_CAPACITY = 205824  // 201K
  def excToOptionWithLog[V](f : => V) : Option[V] = {
    try {
      Some(f)
    } catch {
      case e : Exception => {
        error("excToOptionWithLog", e);
        None
      }
    }
  }

  def excToOption[V](f : => V) : Option[V] = {
    try {
      Some(f)
    } catch {
      case e : Exception => None
    }
  }

  def run(x : => Unit) : Runnable = {
    object anon extends Runnable {
      override def run() {
        val start = java.lang.System.nanoTime()
        x
        val end = java.lang.System.nanoTime()
        //println("Timerec " + Thread.currentThread().getId + " " + start + " " + end)
      }
    }
    anon
  }

  /** A way to force the JVM to exit on uncaught Throwable. */
  class TerminatingExceptionHandler extends Thread.UncaughtExceptionHandler with Logging {
    override def uncaughtException(t : Thread, e : Throwable) : Unit = {
      error("Uncaught Throwable for thread " + t.getName() + " : " + e.getMessage() + ", exiting.", e);
      System.exit(2);
    }
  }

  def writePID(file : String) {
    // Code works only with advisory locks, could be non-portable
    val bo = new Array[Byte](100)
    val cmd = Array("bash", "-c", "echo $PPID")
    val p = Runtime.getRuntime.exec(cmd)
    val insize = p.getInputStream.read(bo)
    val in = excToOption(new java.io.FileOutputStream(file,true))
    if (in != None) {
      val lock = in.get.getChannel.tryLock()
      if (lock == null) throw new java.io.IOException("Process already running - PID file in use, failed PID was " + new String(bo))
      new java.io.File(file).deleteOnExit()
    }
    val out = new java.io.FileOutputStream(file)
    out.write(bo.take(insize))
    out.flush()
    //out.close() // Calling out.close releases the lock WTF!!!
  }

  def unfold[T, R](init: T, f: T => Option[(R, T)]): List[R] =
    f(init).map(x => x._1 :: unfold(x._2,f)).getOrElse(Nil)

  val localIPAddresses = {
    val interfaces = unfold(NetworkInterface.getNetworkInterfaces, { p:java.util.Enumeration[NetworkInterface] =>
      if (p.hasMoreElements) Some(p.nextElement(),p) else None})

    for ( iface  <- interfaces ;
          iaddr  <- iface.getInterfaceAddresses.asScala.map(_.getAddress) ;
          if iaddr.isInstanceOf[Inet4Address])
    yield iaddr.getHostAddress
  }

  def isLocalHost(hostName : String) = {
    val address = InetAddress.getByName(hostName).getHostAddress()
    localIPAddresses.exists(_ == address)
  }

  val INTMAX: Long = Int.MaxValue.toLong
  val HASH_BASE: Long = Int.MaxValue.toLong - Int.MinValue.toLong

  // A SlateUpdater receives a SlateValue.slate of type SlateObject.
  type SlateObject = Object

  def str(a: Array[Byte]) = new String(a)

  def argParser(syntax: Map[String, (Int, String)], args: Array[String]): Map[String, List[String]] = {
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
      Map.empty ++ result
    else
      Map.empty
  }

  class Later[T] {
    var obj: Option[T] = None
    val sem = new Semaphore(0)
    def get(): T = { sem.acquire(); obj.get }
    def set(x: T) { obj = Option(x); sem.release() }
  }

  def fetchURL(urlStr: String): Option[Array[Byte]] = {
    excToOptionWithLog {
       val httpget = new HttpGet(urlStr);
       EntityUtils.toByteArray(HTTPCLIENT.execute(httpget).getEntity())
    }
  }
}

object Mupd8Type extends Enumeration {
  type Mupd8Type = Value
  val Source, Mapper, Updater = Value
}


object GT {

  // wrap up Array[Byte] with Key since Array[Byte]'s comparison doesn't
  // compare array's content which is needed in mupd8
  case class Key(val value: Array[Byte]) {

  override def hashCode() = Arrays.hashCode(value)

    override def equals(other: Any) = other match {
      case that: Key => Arrays.equals(that.value, value)
      case _ => false
    }

    override def toString() = {
      new String(value)
    }
  }

  type Event = Array[Byte]
  type Priority = Int

  val SOURCE_PRIORITY: Priority = 96 * 1024
  val NORMAL_PRIORITY: Priority = 64 * 1024
  val SYSTEM_PRIORITY: Priority = 0
  type TypeSig = (Int, Int) // AppID, PerformerID
}

