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
import com.walmartlabs.mupd8.compression.CompressionFactory
import java.util.concurrent.TimeUnit
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.ThreadPoolExecutor
import scala.collection.JavaConverters._
import scala.collection.JavaConversions._
import scala.util.Try
import scala.util.Failure
import scala.util.Success
import grizzled.slf4j.Logging
import org.scale7.cassandra.pelops.Cluster
import org.scale7.cassandra.pelops.Pelops
import org.scale7.cassandra.pelops.Mutator
import org.apache.cassandra.thrift.ConsistencyLevel
import org.scale7.cassandra.pelops.Bytes
import org.apache.cassandra.thrift.Column
import org.apache.cassandra.thrift.KsDef
import org.apache.cassandra.thrift.CfDef
import org.scale7.cassandra.pelops.KeyspaceManager
import org.scale7.cassandra.pelops.Selector

trait IoPool extends Logging{
  def fetchSlates(name: String, key: Key, next: Option[Array[Byte]] => Unit)
  def initBatchWrite: Boolean = true
  def batchWrite(columnName: String, key: Key, slate: Array[Byte]): Boolean
  def flushBatchWrite: Boolean = true
  def closeBatchWrite: Unit = {}
  def pendingCount = 0
  def fetchStringValueColumn(cfName: String, rowKey: String, colName: String): String = ""
  def writeColumn(cfName: String, rowKey: String, colName: String, value: String) {}
  def shutdown {}
}

class NullPool extends IoPool {
  override def fetchSlates(name: String, key: Key, next: Option[Array[Byte]] => Unit) { next(None) }
  override def batchWrite(columnName: String, key: Key, slate: Array[Byte]): Boolean = true
}

object CassandraPool {
  val PRIMARY_ROWKEY = "__primary"
  val MESSAGE_SERVER = "__message_server"
  val STARTED_SOURCES = "__started_sources"
}

class CassandraPool(
  val hosts: Array[String],
  val port: Int,
  val keyspace: String,
  val getCF: String => String,
  val getTTL: String => Int,
  val compressionCodec: String) extends IoPool {

  // Internal constants help estimate the size of a Thrift operation,
  // which has a hard upper limit.
  //
  // These overhead values are overestimates, not inspections of the
  // actual Thrift protocol:
  val thriftOperationOverheadBytes = 128
  val thriftMutatorOverheadBytes = 128
  // TODO Make configurable: Cassandra actually allows this value to be
  //      configured as thrift_framed_transport_size_in_mb (default 15)
  // As of Cassandra 1.1,12, 1.2.6, thrift_max_message_length_in_mb is gone
  // (see https://issues.apache.org/jira/browse/CASSANDRA-5529).
  val thriftOperationMaximumBytes = 15*1024*1024

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

  Pelops.addPool(poolName, cluster, keyspace)
  val selector = Pelops.createSelector(poolName) // TODO: Should this be per thread?
  val pool = new ThreadPoolExecutor(10, 50, 5, TimeUnit.SECONDS, new LinkedBlockingQueue[Runnable]) // TODO: We can drop events unless we have a Rejection Handler or LinkedQueue

  // fetch slates and call next on slates
  // name: performer name
  // key: slate key
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

  // caller needs to handle cassandra exception
  override def fetchStringValueColumn(cfName: String, rowKey: String, colName: String): String = {
    val col = selector.getColumnFromRow(cfName, rowKey, colName, ConsistencyLevel.QUORUM)
    Selector.getColumnStringValue(col)
  }

  var batchMutator: Mutator = null
  var batchEstimatedWriteSize = 0
  override def initBatchWrite: Boolean = {
    // since writerthread and changing ring might want to flush dirty slates
    // at the same time, use batchMutator as mutex flag
    this.synchronized {
      while (batchMutator != null) this.wait
      batchMutator = Pelops.createMutator(poolName)
      batchEstimatedWriteSize = thriftOperationOverheadBytes
      true
    }
  }

  override def batchWrite(columnName: String, key: Key, slate: Array[Byte]): Boolean = {
    val compressed = cService.compress(slate)
    val columnFamily = getCF(columnName)
    val columnNameBytes = Bytes.fromByteArray(columnName.getBytes)
    val columnValue = Bytes.fromByteArray(compressed)

    // Overestimate columnFamily by imagining each character as a
    // max-length (4-byte) UTF-8 sequence [IETF STD 63/RFC 3629]
    // Refine thisOperationSize by reducing mutator overhead, counting TTL.
    val thisOperationSize = thriftMutatorOverheadBytes + 4*columnFamily.length + key.value.size + columnNameBytes.length + columnValue.length
    // debug("Trying to add write "+thisOperationSize+" to batch size "+batchEstimatedWriteSize)
    if (batchEstimatedWriteSize + thisOperationSize > thriftOperationMaximumBytes) {
      // debug("Add to batch rejected")
      false
    } else {
      batchMutator.writeColumn(
        columnFamily,
        Bytes.fromByteArray(key.value),
        // batchMutator.newColumn(Bytes.fromByteArray(columnName.getBytes), Bytes.fromByteArray(compressed), getTTL(columnName)))
        batchMutator.newColumn(columnNameBytes, columnValue, getTTL(columnName)))
      batchEstimatedWriteSize += thisOperationSize
      true
    }
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
