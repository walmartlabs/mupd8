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

package com.walmartlabs.mupd8.application;

import java.util.NoSuchElementException;

public interface Mupd8Source {
  /**
   * Returns true if there is a next item ready to be read by getNextDataPair().
   * @return true if there is a next item ready to be read by getNextDataPair(), false otherwise
   */
	boolean         hasNext();

  /**
   * Returns a Mupd8DataPair parsed from the next item
   * @return a Mupd8DataPair parsed from the next item
   * @throws NoSuchElementException if there is no next item to read from
   */
	Mupd8DataPair   getNextDataPair() throws NoSuchElementException;
}
