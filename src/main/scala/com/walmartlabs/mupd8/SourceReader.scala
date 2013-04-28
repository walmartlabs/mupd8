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

import com.walmartlabs.mupd8.application.Mupd8Source
import com.walmartlabs.mupd8.application.Mupd8DataPair
import com.walmartlabs.mupd8.Misc._
import java.io.BufferedReader
import java.net.Socket
import java.io.InputStreamReader
import java.io.FileReader
import java.lang.Exception
import org.codehaus.jackson._
import org.codehaus.jackson.map.ObjectMapper
import annotation.tailrec
import java.util.Random
import grizzled.slf4j.Logging

/** A default JSON Source Reader
 *
 * @constructor create a JSON Source Reader with source and key path in json
 * @param source source either a socket or a file
 * @param key path of key in json line
 * @note This implementation is not thread-safe in the sense that only one consumer thread can interact with this object 
 *
 */
class JSONSource (args : java.util.List[String]) extends Mupd8Source with Logging {
  val sourceStr = args.get(0)
  val keyStr = args.get(1)
  val sourceArr = sourceStr.split(":")
  private var _reader = constructReader
  private val _random = new Random(System.currentTimeMillis());
  private val _reconnectOnEof = sourceArr(0) match {
    case "file" => false
    case _      => true
  }

  private var _currentLine : Option[String] = None
  private val objMapper = new ObjectMapper

  def constructReader : Option[BufferedReader] = {
    sourceArr(0) match {
      case "file" => fileReader
      case _      => socketReader
    }
  }

  def fileReader : Option[BufferedReader] = {
    try {
      Some(new BufferedReader(new FileReader(sourceArr(1))))
    } catch {
      case e: Exception => {error("JSONSource: fileReader hit exception", e)
                            None}
    }
  }

  def socketReader : Option[BufferedReader] = {
    try {
      info("JSONSource: connecting to "+sourceArr(0)+" port "+sourceArr(1))
      val socket = new Socket(sourceArr(0), sourceArr(1).toInt)
      Some(new BufferedReader(new InputStreamReader(socket.getInputStream())))
    } catch {
      case e: Exception => {error("JSONSource: socketReader hit exception", e)
                            None}
    }
  }

  def getValue(key: String, node: JsonNode) : Option[JsonNode] = {
    try {
      Some(key.split(':').foldLeft(node)((m, k) => m.path(k)))
    } catch {
      case e: Exception => {error("JSONSource: getValue hit exception with ndoe = " + node.toString, e)
                            None}
    }
  }

  override def hasNext() : Boolean = {
    _currentLine = _currentLine.orElse({
        try {
          readLine
        } catch {
          case e : Exception => {error("JSONSource: reader readLine failed", e)
                                 destroyReader
                                 None}
        }
      })

    _currentLine isDefined
  }

  private final def readLine() : Option[String] = {
    return readLine(0)
  }

  @tailrec
  private final def readLine(retryCount : Int) : Option[String] = {
    Option(_reader.get.readLine) match {
      case Some(x) => Some(x)
      case None => {
        destroyReader
        if (! _reconnectOnEof) {
          None
        } else {
          // Sleep exponential time, upper bounded 
          val maxSleepTime = 1 << (10 + (if (retryCount > 6) 6 else retryCount))
          Thread.sleep(_random.nextInt(maxSleepTime))
          // Reconstruct reader and read
          _reader = constructReader
          readLine(retryCount + 1)
        }
      }
    }
  }

  private def destroyReader : Unit = {
    _reader = _reader.flatMap(r => {r.close; None})
  }

  @throws(classOf[NoSuchElementException])
  override def getNextDataPair: Mupd8DataPair = {
    if (hasNext) {
      val rtn = new Mupd8DataPair
      val key = getValue(keyStr, objMapper.readTree(_currentLine.get))
      assert(key != None, "key from source is wrong: " + _currentLine)
      rtn._key = key.get.asText
      rtn._value = new String(_currentLine.get).getBytes()
      _currentLine = None
      rtn
    } else {
      throw new NoSuchElementException("JSONSource doesn't have next data pair")
    }
  }
}
