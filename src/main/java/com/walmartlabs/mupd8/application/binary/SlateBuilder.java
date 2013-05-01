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

import java.io.OutputStream;

/** Serialize and deserialize slates of some type into byte[].
 */
public interface SlateBuilder {

	/** Serialize slate into bytes. */
	public boolean toBytes(Object slate, OutputStream out);

	/** Deserialize bytes into a slate. */
	public Object toSlate(byte[] bytes);
}