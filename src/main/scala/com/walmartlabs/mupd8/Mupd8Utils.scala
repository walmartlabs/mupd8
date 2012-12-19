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


object Mupd8Utils {

  def getHostNameToIndex(staticInfo: AppStaticInfo, host: String): Int = {
    val index = {
      staticInfo.systemHosts.zipWithIndex.find {
        case (h, i) =>
          getIPAddress(h) == host
      }.get._2
    }
    index
  }



  def hash2Float(key: Any): Float = {
    val INTMAX: Long = Int.MaxValue.toLong
    val HASH_BASE: Long = Int.MaxValue.toLong - Int.MinValue.toLong
    (key.hashCode.toLong + INTMAX).toFloat / HASH_BASE
  }

}
