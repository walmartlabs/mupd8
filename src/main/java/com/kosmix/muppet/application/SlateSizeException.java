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

/** The slate being constructed is of a disallowed size.
 * 
 *  This Exception is thrown to warn that a Slate is too large,
 *  and will be discarded in its current state.
 */
public class SlateSizeException extends Exception {
	private static final long serialVersionUID = -4088317439305528023L;

	/** The actual size of the disallowed slate, in bytes. */
	public int actualLength;
	public int allowedLength;

	public SlateSizeException(int length, int max) {
		actualLength = length;
		allowedLength = max;
	}

	@Override
	public String toString() {
		return this.getClass().getSimpleName()+": Actual length "+actualLength+" exceeds permitted length "+allowedLength+")";
	}

	@Override
	public String getMessage() {
		return toString();
	}
}
