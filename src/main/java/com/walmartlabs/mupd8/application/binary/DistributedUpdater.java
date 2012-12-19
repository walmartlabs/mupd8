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
 * This Updater either updates a slate given an event, or merges two slates.
 *
 * As in an ordinary Updater, any event passed in is considered handled
 * (and not redistributed to any other instance of the Updater).
 *
 * Unlike an ordinary Updater, multiple slates may be provided, but like
 * an ordinary Updater, only one slate may be published (repeated publish
 * calls overwrite the slate to publish).
 *
 * @see UnifiedUpdater
 */
public interface DistributedUpdater extends Performer {
    /** Update method for incoming event.
	 *
	 *  @param submitter - interface through which to publish updated slate or additional events
	 *  @param stream    - name of stream on which event arrived
	 *  @param key       - key for event and slate
	 *  @param event     - event value (payload)
	 *  @param slate     - slate to update
     */
	void update(PerformerUtilities submitter, String stream, byte[] key, byte[] event, Slate slate);

	/** Update method that merges two slates into one.
	 *
	 *  @param submitter - interface through which to publish merged slate or additional events
	 *  @param key       - key for slate
	 *  @param slateA    - slate to merge (in no particular order)
	 *  @param slateB    - slate to merge (in no particular order)
	 */
	void merge(PerformerUtilities submitter, byte[] key, Slate slateA, Slate slateB);
}
