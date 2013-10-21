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

import com.walmartlabs.mupd8.Misc._
import com.walmartlabs.mupd8.GT._
import com.walmartlabs.mupd8.Misc._
import grizzled.slf4j.Logging
import com.walmartlabs.mupd8.compression.CompressionFactory
import org.scale7.cassandra.pelops.Cluster
import org.scale7.cassandra.pelops.Pelops
import org.scale7.cassandra.pelops.Mutator
import java.util.concurrent.TimeUnit
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.ThreadPoolExecutor
import scala.collection.JavaConverters._
import scala.collection.JavaConversions._
import org.apache.cassandra.thrift.ConsistencyLevel
import org.scale7.cassandra.pelops.Bytes
import org.apache.cassandra.thrift.Column
import org.apache.cassandra.thrift.KsDef
import scala.util.Try
import org.apache.cassandra.thrift.CfDef
import org.scale7.cassandra.pelops.KeyspaceManager
import scala.util.Failure
import scala.util.Success
import scala.util.Failure
import org.scale7.cassandra.pelops.Selector

trait IoPool extends Logging{
  def fetchSlates(name: String, key: Key, next: Option[Array[Byte]] => Unit)
  def initBatchWrite: Boolean = true
  def batchWrite(columnName: String, key: Key, slate: Array[Byte]): Boolean
  def flushBatchWrite: Boolean = true
  def closeBatchWrite: Unit = {}
  def pendingCount = 0
  def fetchStringValueColumn(cfName: String, rowKey: String, colName: String): Option[String] = None
  def writeColumn(cfName: String, rowKey: String, colName: String, value: String) {}
  def shutdown {}
}

class NullPool extends IoPool {
  def fetchSlates(name: String, key: Key, next: Option[Array[Byte]] => Unit) { next(None) }
  def batchWrite(columnName: String, key: Key, slate: Array[Byte]): Boolean = true
}

object CassandraPool {
  val SETTINGS_CF = "settings"
  val PRIMARY_ROWKEY = "primary"
  val MESSAGE_SERVER = "message_server"
  val STARTED_SOURCES = "started_sources"
}

class CassandraPool(
  val hosts: Array[String],
  val port: Int,
  val keyspace: String,
  val getCF: String => String,
  val getTTL: String => Int,
  val compressionCodec: String) extends IoPool {

  val poolName = keyspace
  val cluster = new Cluster(hosts.reduceLeft(_ + "," + _), port)
  val cService = CompressionFactory.getService(compressionCodec)
  info("Use compression codec " + compressionCodec)
  val keyspaceManager = Pelops.createKeyspaceManager(cluster)
  // try connecting cassandra server
  Try(keyspaceManager.getKeyspaceNames()) match {
    case Failure(ex) =>
      error("CassandraPool: connecting cassandra failed, exiting...", ex)
      System.exit(-1)
    case Success(_) => info("CassandraPool: cassandra server connected.")
  }

  Pelops.addPool(poolName, cluster, keyspace);
  val selector = Pelops.createSelector(poolName) // TODO: Should this be per thread?
  val pool = new ThreadPoolExecutor(10, 50, 5, TimeUnit.SECONDS, new LinkedBlockingQueue[Runnable]) // TODO: We can drop events unless we have a Rejection Handler or LinkedQueue

  // fetch slates and call next on slates
  override def fetchSlates(name: String, key: Key, next: Option[Array[Byte]] => Unit) {
    pool.submit(run {
      val start = java.lang.System.nanoTime()
      val col = excToOption(selector.getColumnFromRow(getCF(name), Bytes.fromByteArray(key.value), Bytes.fromByteArray(name.getBytes), ConsistencyLevel.QUORUM))
      next(col.map { col =>
        assert(col != null)
        val ba = Bytes.fromByteBuffer(col.value).toByteArray
        cService.uncompress(ba)
      })
    })
  }

  override def fetchStringValueColumn(cfName: String, rowKey: String, colName: String): Option[String] = {
    Try(selector.getColumnFromRow(cfName, rowKey, colName, ConsistencyLevel.QUORUM)) match {
      case Success(col) => Some(Selector.getColumnStringValue(col))
      case Failure(ex) =>
        error("fetchStringValueColumn failed", ex)
        None
    }
  }

  var batchMutator: Mutator = null
  override def initBatchWrite: Boolean = {
    // since writerthread and changing ring might want to flush dirty slates
    // at the same time, use batchMutator as mutex flag
    this.synchronized {
      while (batchMutator != null) this.wait
      batchMutator = Pelops.createMutator(poolName)
      true
    }
  }

  override def batchWrite(columnName: String, key: Key, slate: Array[Byte]): Boolean = {
    val compressed = cService.compress(slate)
    batchMutator.writeColumn(
      getCF(columnName),
      Bytes.fromByteArray(key.value),
      batchMutator.newColumn(Bytes.fromByteArray(columnName.getBytes), Bytes.fromByteArray(compressed), getTTL(columnName)))
    true
  }

  override def flushBatchWrite: Boolean = {
    excToOptionWithLog { batchMutator.execute(ConsistencyLevel.QUORUM) } != None
  }

  override def closeBatchWrite {
    this.synchronized {
      batchMutator = null
      this.notifyAll
    }
  }

  override def writeColumn(cfName: String, rowKey: String, colName: String, value: String) {
    val mutator = Pelops.createMutator(poolName)
    mutator.writeColumn(cfName, rowKey, mutator.newColumn(colName, value))
    mutator.execute(ConsistencyLevel.QUORUM)
  }

  override def pendingCount = pool.getQueue.size + pool.getActiveCount

  override def shutdown {Pelops.shutdown()}
}
