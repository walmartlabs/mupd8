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

package com.walmartlabs.mupd8.application.binary;



/** An Updater component of an application.
 *
 * This Updater takes all incoming events and slates and folds/merges them.
 *
 * As in an ordinary Updater, any event passed in is considered handled
 * (and not redistributed to any other instance of the Updater).
 *
 * Unlike an ordinary Updater, multiple slates may be provided, but only
 * one slate may be published (repeated publish calls overwrite the slate
 * to publish).
 */
public interface UnifiedUpdater extends Performer {
    /** Update method that combines all provided events and slates.
     *
     *  @param submitter - interface through which to publish updated slate or additional events
	 *  @param stream    - name of stream on which event(s) arrived (if any, or null if no events)
	 *  @param key       - key for event(s) and slate(s)
     *  @param events    - set of zero or more event payloads for key
     *                    (not a Set<byte[]> because event payloads are
     *                     count-preserving, so they need not be unique)
     *  @param slates    - set of zero or more slates for key
     *                    (not a Set<byte[]> because slates to be merged
     *                     need not be unique)
     */
	void update(PerformerUtilities submitter, String stream, byte[] key, byte[][] events, Slate[] slates);
}
