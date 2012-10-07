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
import org.codehaus.jackson._
import org.codehaus.jackson.map.ObjectMapper

/** A default JSON Source Reader
 *
 * @constructor create a JSON Source Reader with source and key path in json
 * @param source source either a socket or a file
 * @param key path of key in json line
 *
 */
class JSONSource (args : java.util.List[String]) extends Mupd8Source {
  val sourceStr = args.get(0)
  val keyStr = args.get(1)
  val sourceArr = sourceStr.split(":")
  val reader = sourceArr(0) match {
    case "file" => try {fileReader} catch { case _ => println("Source failed. : " + sourceStr + " " + keyStr); null}
    case _      => try {socketReader} catch { case _ => println("Source failed. : " + sourceStr + " " + keyStr); null}
  }

  private var currentLine : String = null
  private val objMapper = new ObjectMapper

  def fileReader : BufferedReader = {
    new BufferedReader(new FileReader(sourceArr(1)))
  }

  def socketReader : BufferedReader = {
    val socket = new Socket(sourceArr(0), sourceArr(1).toInt)
    new BufferedReader(new InputStreamReader(socket.getInputStream()))
  }

  def getValue(key: String, node: JsonNode) : Option[JsonNode] = {
    try {
      Some(key.split(':').foldLeft(node)((m, k) => m.path(k)))
    } catch {
      case e: Exception => {println("ndoe = " + node.toString); e.printStackTrace(); None}
    }
  }
  
  override def hasNext() = {
    if (reader == null) {println("readder is null"); false}
    currentLine = try { reader.readLine() } catch { case e: Exception => {println("reader.readLine returns null");null} }
    currentLine != null
  }

  override def getNextDataPair: Mupd8DataPair = {
    try {
      assert(currentLine != null, "Input line couldn't be null in getNextDataPair: " + currentLine)
      val rtn = new Mupd8DataPair
      val key = getValue(keyStr, objMapper.readTree(currentLine))
      assert(key != None, "key from source is wrong: " + currentLine)
      rtn._key = key.get.asText
      rtn._value = new String(currentLine).getBytes()
      rtn
    } catch {
       case e: Exception => {e.printStackTrace; null}
    }
  }
}
