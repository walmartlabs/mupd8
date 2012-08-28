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

class SourceReader(uri : String, continuation : Array[Byte] => Unit)
      extends Runnable {
  import java.io.InputStream
  import java.io.BufferedInputStream
  
  val arr = uri.split(":")
 
  def read(istream : InputStream) : Unit = {
    var run : List[Byte] = Nil
    var b = 0      
    while ({
      run = Nil
      while ({
        b = istream.read()
        b != -1 && b.toChar != '\n'        
      }) { run = b.toByte::run }
      b != -1
    }) try {
      continuation(run.toArray.reverse)
    } catch {
      case e : Exception => println("Discarding event because source processing failed with "+e.toString+" on source event "+new String(run.toArray.reverse, "UTF-8"))
    }
  }
  
  def readFile (file : String) : Unit = {
    println("read from " + file)
    try {
      val istream = new BufferedInputStream(new java.io.FileInputStream(file))
      read(istream)
      istream.close
    } catch {
      case e : Exception => println(e.getMessage)
    }
  }
  
  def readSocket (host: String, port : Int) : Unit = {
    import java.net._
    
    var socket : Socket = null
    var reading = true
    while (reading) {
      try {
        socket = new Socket(host, port)
        println("connected to " + host + ":" + port)
        val istream = new BufferedInputStream(socket.getInputStream)
        read(istream)
      } catch {
        case ioe : java.io.IOException => {
          println(ioe.getMessage + " IOException, sleep a while and try again")
          Thread.sleep(1000)
        }
        case e : Exception => {
          println("quit reader on exception " + e.toString + ": " + e.getMessage)
          e.printStackTrace
          if (socket != null && socket.isConnected) socket.close
          reading = false
        }
      }
    }
  }
  
  def run {
    arr(0) match {
      case "file" => readFile(arr(1))
      case _      => readSocket(arr(0), arr(1).toInt)
    }
  }
}
