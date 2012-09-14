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

import scala.util.parsing.json.JSON
import com.walmartlabs.mupd8.application.Mupd8Source
import com.walmartlabs.mupd8.application.Mupd8DataPair
import com.walmartlabs.mupd8.Misc._
import java.io.BufferedReader
import java.net.Socket
import java.io.InputStreamReader
import java.io.FileReader

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

  var currentLine : String = null;

  def fileReader : BufferedReader = {
    new BufferedReader(new FileReader(sourceArr(1)))
  }

  def socketReader : BufferedReader = {
    val socket = new Socket(sourceArr(0), sourceArr(1).toInt)
    new BufferedReader(new InputStreamReader(socket.getInputStream()))
  }

  def getValue(key: String, map: Option[Any]) : Option[Any] =
    key.split(':').foldLeft(map)((m, k) => m.asInstanceOf[Option[Map[String, Any]]].map{_.get(k)}.get )

  override def hasNext() = {
    if (reader == null) false
    currentLine = reader.readLine()
    currentLine != null
  }

  override def getNextDataPair: Mupd8DataPair = {
    val rtn = new Mupd8DataPair
    val json = JSON.parseFull(currentLine)
    rtn._key = new String(getValue(keyStr, json).get.asInstanceOf[String])
    rtn._value = new String(currentLine).getBytes()
    rtn
  }
}
