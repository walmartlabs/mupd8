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

package com.walmartlabs.mupd8.examples

import org.json._
import org.json.simple.JSONValue
import com.walmartlabs.mupd8.application._
import com.walmartlabs.mupd8.application.Config
import com.walmartlabs.mupd8.application.binary._

class T10Mapper(config : Config, val name : String) extends Mapper {    
  override def getName = name
    
  val streams = Map( "k1" -> "K1Stream", "k2" -> "K2Stream", "k3" -> "K3Stream", "k4" -> "K4Stream" )
  
  override def map(perfUtil : PerformerUtilities, stream : String, key : Array[Byte], event : Array[Byte]) {
    val json = new JSONObject(new String(event, "UTF-8"))
    streams foreach { case(key,stream) =>
      perfUtil.publish(stream, json.getString(key).getBytes("UTF-8"), event) 
    }
  }
}

class TestSlate(jsonParam: Option[JSONObject]) extends Slate {
    val json:JSONObject = jsonParam.map {p => p}.getOrElse(new JSONObject)
  
    override def toBytes() = {
      json.toString().getBytes()
    }
    
    override def getBytesSize() = {
      0
    }
    
}

class KnUpdater (config : Config, val name : String) extends Updater {
  override def getName = name
  
  override def toSlate(bytes : Array[Byte]) = {
    val json = (JSONValue.parseWithException(new String(bytes, "UTF-8"))).asInstanceOf[JSONObject]
    new TestSlate(Option(json))
  }

  override def update(perfUtil : PerformerUtilities, stream : String, key : Array[Byte], event : Array[Byte], slate : Slate) {
    val testSlate = slate.asInstanceOf[TestSlate]
    val slatej = testSlate.json
    val eventj = new JSONObject(new String(event, "UTF-8"))
    val count = slatej.optInt("counter",0) + 1
    slatej.put("counter", count)
    if (!slatej.has("string_test"))
      slatej.put("string_test", eventj.getString("string_test"))
    if (!slatej.has("key"))
      slatej.put("key", new String(key, "UTF-8"))
    perfUtil.replaceSlate(testSlate)
  }
  
  override def getDefaultSlate() = {
    new TestSlate(None)
  }
}
