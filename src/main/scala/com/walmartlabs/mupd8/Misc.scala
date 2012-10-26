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

import scala.collection.JavaConverters._

object Misc {

  val SLATE_CAPACITY = 1048576 // 1M size
  val COMPRESSED_CAPACITY = 205824  // 201K
  def excToOptionWithLog[V](f : => V) : Option[V] = {
    try {
      Some(f)
    } catch {
      case e : Exception => {
        e.printStackTrace(new java.io.PrintStream(System.out)) ;
        println("excToOptionWithLog " + e) ;
        None
      }
    }
  }

  def excToOption[V](f : => V) : Option[V] = {
    try {
      Some(f)
    } catch {
      case _ : Exception => None
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
  class TerminatingExceptionHandler extends Thread.UncaughtExceptionHandler {
    override def uncaughtException(t : Thread, e : Throwable) : Unit = {
      println("Uncaught Throwable for thread " + t.getName() + " : " + e.getMessage() + ", exiting.");
      e.printStackTrace(new java.io.PrintStream(System.out));
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

  def getIPAddress(hostName : String) : String = { InetAddress.getByName(hostName).getHostAddress() }

  def isLocalHost(hostName : String) = {
    val address = getIPAddress(hostName)
    localIPAddresses.exists(_ == address)
  }
}
