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

import java.util.concurrent.PriorityBlockingQueue
import scala.collection.breakOut
import grizzled.slf4j.Logging
import com.walmartlabs.mupd8.application._
import com.walmartlabs.mupd8.GT._
import com.walmartlabs.mupd8.Misc._
import com.walmartlabs.mupd8.Mupd8Type._

class TLS(val appRun: AppRuntime) extends binary.PerformerUtilities with Logging {
  val objects = appRun.appStatic.performerFactory.map(_.map(_.apply()))
  val slateCache = new SlateCache(appRun.storeIO, appRun.appStatic.slateCacheCount, this)
  val queue = new PriorityBlockingQueue[Runnable]
  var perfPacket: PerformerPacket = null
  var startTime: Long = 0

  val unifiedUpdaters: Set[Int] =
    (for (
      (oo, i) <- objects.zipWithIndex;
      o <- oo;
      if excToOption(o.asInstanceOf[binary.UnifiedUpdater]) != None
    ) yield i)(breakOut)

  override def publish(stream: String, key: Array[Byte], event: Array[Byte]) {
    trace("TLS::Publish: Publishing to " + stream + " Key " + str(key) + " event " + str(event))
    appRun.appStatic.edgeName2IDs.get(stream).map(_.foreach(
      pid => {
        trace("TLS::publish: Publishing to " + appRun.appStatic.performers(pid).name)
        val packet = PerformerPacket(NORMAL_PRIORITY, pid, Key(key), event, stream, appRun)
        if (appRun.appStatic.performers(pid).mtype == Mapper)
          appRun.pool.put(packet)  // publish to mapper?
        else
          appRun.pool.put(packet.getKey, packet)
      })).getOrElse(error("publish: Bad Stream name" + stream))
  }

  //  import com.walmartlabs.mupd8.application.SlateSizeException
  @throws(classOf[SlateSizeException])
  override def replaceSlate(slate: SlateObject) {
    // TODO: Optimize replace slate to avoid hash table look ups
    val name = appRun.appStatic.performers(perfPacket.pid).name
    assert(appRun.appStatic.performers(perfPacket.pid).mtype == Updater)
    val cache = appRun.getTLS(perfPacket.pid, perfPacket.slateKey).slateCache
    trace("replaceSlate " + appRun.appStatic.performers(perfPacket.pid).name + "/" + perfPacket.slateKey + " Oldslate " + (cache.getSlate((name,perfPacket.slateKey)).get).toString() + " Newslate " + slate.toString())
    cache.put((name, perfPacket.slateKey), slate)
  }
}
