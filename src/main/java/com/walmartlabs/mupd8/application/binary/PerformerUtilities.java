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

import com.walmartlabs.mupd8.application.SlateSizeException;

/** A Performer gets an object of type PerformerUtilities to receive and publish outgoing events.
 * 
 * This class is so named to allow Mupd8 implementations to provide additional services
 * accessible to a Performer.
 * 
 * A Performer should use EventPublisher.publish (only) to post a new event.
 */
public interface PerformerUtilities {
	/** Direct version of publish (recommended for new code).
	 * 
	 * @param stream - name of stream to which to publish event
	 * @param key - event key
	 * @param event - event data
	 * @throws Exception - reserved (may be used to indicate null values or disallowed stream)
	 *
	 * @todo TODO Throw more specific exceptions (indicating an invalid value).
	 */
	public void publish(String stream, byte[] key, byte[] event) throws Exception;
	/** Replace a given slate (updaters only).
	 *
	 * @param slate - new slate to replace existing slate (if updater; if mapper, ignored)
	 */
	public void replaceSlate(Slate slate) throws SlateSizeException;
}
