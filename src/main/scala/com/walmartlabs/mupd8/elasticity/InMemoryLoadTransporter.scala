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

import com.walmartlabs.mupd8.elasticity.ElasticOracle
import com.walmartlabs.mupd8.elasticity.LoadTransporter
import com.walmartlabs.mupd8.AppRuntime

class InMemoryLoadTransporter extends LoadTransporter {

  
  def transferSlates(oracle: ElasticOracle) = {
    val appRuntime=RuntimeProvider.appRuntime
    val tls = appRuntime.getTLS
    val slateCache = tls.getSlateCache
    val items = tls.slateCache.getAllItems
    items.zipWithIndex.foreach {
      case ((key, item), i) =>
        if (oracle.isMovingKey(key.getBytes())) {
          System.out.println(" candidate slate :" + item.slate)
        }
    }
  }

}
