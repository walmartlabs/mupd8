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
package com.walmartlabs.mupd8.elasticity

import java.util.concurrent.atomic.AtomicBoolean
import scala.collection.mutable.HashMap
import java.util.ArrayList
import com.walmartlabs.mupd8.Mupd8Utils
import com.walmartlabs.mupd8.application.binary.Slate
import com.walmartlabs.mupd8.application.binary.Updater;
import com.walmartlabs.mupd8.application.statistics.PrePerformer
import com.walmartlabs.mupd8.application.binary.PerformerUtilities

class ElasticWrapper(val updater: Updater, prePerformer: PrePerformer) extends Updater {

  val WRAPPER_SUFFIX = "_elastic_wrapper"
  var loadRedistributionInProgress = false
  var oracle: ElasticOracle = null
  var buffer: HashMap[Float, ArrayList[(String, Array[Byte], Array[Byte])]] = new HashMap[Float, ArrayList[(String, Array[Byte], Array[Byte])]] //TODO concurrent hash map
  var keyHashes: HashMap[Float, Array[Byte]] = new HashMap[Float, Array[Byte]]
  var submitter: PerformerUtilities = null;

  override def getName(): String = {
    updater.getName() + WRAPPER_SUFFIX;
  }

  def getBufferedEvents() = { buffer }
  def getBufferedKeyHashes() = { keyHashes }

  def setLoadRedistributionInProgress(oracle: ElasticOracle): Unit = {
    this.oracle = oracle
    loadRedistributionInProgress = true
    //TODO: ensure proper synchronization
  }

  def setLoadRedistStateTransferCompleted(): Unit = {
    // before we begin emptying the buffer, it must be guaranteed that 
    // the buffer is bounded that is no  more traffic is being routed to this buffer
    for ((key, events) <- buffer) {
      val it = events.iterator()
      while (it.hasNext()) {
        val event = it.next
        submitter.publish(event._1, event._2, event._3)
      }
    }
    buffer.clear()
  }

  def actualUpdate(submitter: PerformerUtilities, stream: String, key: Array[Byte], event: Array[Byte], slate: Slate) = {
    prePerformer.prePerform(key, event)
    updater.update(submitter, stream, key, event, slate)
  }

  override def update(submitter: PerformerUtilities, stream: String, key: Array[Byte], event: Array[Byte], slate: Slate) = {
    if (loadRedistributionInProgress && oracle.isMovingKey(key)) {
      var fl = Mupd8Utils.hash2Float(key)
      keyHashes += (fl -> key)
      var obj = buffer.getOrElse(fl, null)
      if (obj == null) {
        var list = new ArrayList[(String, Array[Byte], Array[Byte])]
        list.add((stream, key, event))
        buffer.put(fl, list)
      } else {
        obj.asInstanceOf[ArrayList[Array[Byte]]].add(event)
        buffer.put(fl, obj)
      }
    } else {
      actualUpdate(submitter, stream, key, event, slate)
    }
  }
  
  override def toSlate(bytes : Array[Byte]) = {
    updater.toSlate(bytes)
  }
  
  override def getDefaultSlate() = {
    updater.getDefaultSlate()
  }

}
