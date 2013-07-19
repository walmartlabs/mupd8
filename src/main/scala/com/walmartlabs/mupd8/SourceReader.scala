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
import java.io.IOException
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

  var _reader = constructReader
  val _random = randomGenerator
  val _reconnectOnEof = reconnectable(sourceArr(0))
  private var _currentLine : Option[String] = None
  private val objMapper = new ObjectMapper

  def constructReader : BufferedReader = {
    sourceArr(0) match {
      case "file" => fileReader
      case _      => _ensureSocketReaderCreated()
    }
  }

  def fileReader : BufferedReader = {
    new BufferedReader(new FileReader(sourceArr(1)))
  }

  def socketReader : BufferedReader = {
    val socket = new Socket(sourceArr(0), sourceArr(1).toInt)
    new BufferedReader(new InputStreamReader(socket.getInputStream()))
  }

  def reconnectable(sourceType: String): Boolean = {
    sourceType match {
        case "file" => false
        case _      => true
      }
  }

  def randomGenerator: Random = {
    new Random(System.currentTimeMillis())
  }

  /**
   * Ensure to return a concrete socket reader. 
   * 
   * @note This method implements exponential backoff until successfully return a socket reader.
   */
  @tailrec
  final def _ensureSocketReaderCreated(retryCount: Int = 0): BufferedReader = {
    info("Connecting to "+sourceArr(0)+" port "+sourceArr(1))
    val result: Option[BufferedReader] = try {
      Option(socketReader)
    } catch {
      case e: Exception => {
        warn("Failed to connect to "+sourceArr(0)+" port "+sourceArr(1)+". Retrying...")
        None
      }
    }
    result match {
      case Some(x) => x
      case None => {
        // Sleep exponential time, upper bounded
        val maxSleepTime = 1 << (10 + (if (retryCount > 6) 6 else retryCount))
        Thread.sleep(_random.nextInt(maxSleepTime))
        _ensureSocketReaderCreated()
      }
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
        _readLine()
      } catch {
        case e : Exception =>
          error("JSONSource: reader readLine failed", e)
          _ensureReaderClosed
          None
      }
    })
    _currentLine.isDefined
  }

  @tailrec
  final def _readLine(retryCount : Int = 0) : Option[String] = {
    val result: Option[String] = try {
      Option(_reader.readLine)
    } catch {
      case e: Exception => {
        _reconnectOnEof match {
          case true => warn("ReadLine error, reconnecting...")
          case false => throw new IOException("ReadLine error, disconnecting...", e)
        }
        None
      }
    }
    result match {
      case Some(x) => Some(x)
      case None => {
        _ensureReaderClosed
        _reconnectOnEof match {
          case false => None
          case true => {
            // Reconstruct reader and read
            _reader = constructReader
            _readLine(retryCount + 1)
          }
        }
      }
    }
  }
    
  def _ensureReaderClosed: Unit = {
    try {
      _reader.close;
    } catch {
      case e: IOException => {
        error("JSONSource: error in closing reader.")
      }
    }
  }

  override def getNextDataPair: Mupd8DataPair = {
    if (hasNext) {
      try {
        val rtn = new Mupd8DataPair
        val key = getValue(keyStr, objMapper.readTree(_currentLine.get))
        assert(key != None, "key from source is wrong: " + _currentLine)
        rtn._key = key.get.asText
        rtn._value = new String(_currentLine.get).getBytes()
        _currentLine = None
        rtn
      } catch {
        case e: Exception => throw new NoSuchElementException("JSONSource doesn't have next data pair")
      } finally {
        _currentLine = None
      }
    } else {
      // clean _currentLine
      _currentLine = None
      throw new NoSuchElementException("JSONSource doesn't have next data pair")
    }
  }
}
