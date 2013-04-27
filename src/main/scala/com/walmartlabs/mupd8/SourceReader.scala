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
 * @warning This implementation is not thread-safe in the sense that only one consumer thread can interact with this object 
 *
 */
class JSONSource (args : java.util.List[String]) extends Mupd8Source with Logging {
  val sourceStr = args.get(0)
  val keyStr = args.get(1)
  val sourceArr = sourceStr.split(":")
  private var reader = constructReader
  private val random = new Random(System.currentTimeMillis());
  val reconnectOnEof = sourceArr(0) match {
    case "file" => false
    case _      => true
  }

  private var currentLine : String = null
  private val objMapper = new ObjectMapper

  def constructReader : BufferedReader = {
    sourceArr(0) match {
      case "file" => fileReader
      case _      => socketReader
    }
  }

  def fileReader : BufferedReader = {
    try {
      new BufferedReader(new FileReader(sourceArr(1)))
    } catch {
      case e: Exception => {error("JSONSource: fileReader hit exception", e)
                            null}
    }
  }

  def socketReader : BufferedReader = {
    try {
      info("JSONSource: connecting to "+sourceArr(0)+" port "+sourceArr(1))
      val socket = new Socket(sourceArr(0), sourceArr(1).toInt)
      new BufferedReader(new InputStreamReader(socket.getInputStream()))
    } catch {
      case e: Exception => {error("JSONSource: socketReader hit exception", e)
                            null}
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
    if (currentLine == null) {
      currentLine = readLine(reconnectOnEof, 0)
    }
    return currentLine != null
  }

  @tailrec
  private final def readLine(repeat : Boolean, repeatTimes: Int) : String = {
    val rlt: String = if (reader == null) {
      if (repeat) {
        val sleepTime = if (repeatTimes > 6) 6 else repeatTimes;
        // sleep exponential time
        Thread.sleep(random.nextInt(1 << (10 + sleepTime)))
        reader = constructReader
        // need to call readLine again to double check if reader is well constructed
        null
      } else null
    } else {
      try {
        val line = reader readLine;
        if (line == null) {reader close; reader = null}
        line
      } catch {
        case e: Exception => error("JSONSource: reader readLine failed", e)
        if (reader != null) {reader close; reader = null}
        null
      }
    }

    if (rlt != null) rlt
    else if (!repeat) {
      if (reader != null) {reader close; reader = null}
      null
    } else {
      readLine(repeat, repeatTimes + 1)
    }
  }

  @throws(classOf[NoSuchElementException])
  override def getNextDataPair: Mupd8DataPair = {
    if (hasNext) {
      val rtn = new Mupd8DataPair
      val key = getValue(keyStr, objMapper.readTree(currentLine))
      assert(key != None, "key from source is wrong: " + currentLine)
      rtn._key = key.get.asText
      rtn._value = new String(currentLine).getBytes()
      currentLine = null
      rtn
    } else {
      throw new NoSuchElementException("JSONSource doesn't have next data pair")
    }
  }
}
