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

package com.walmartlabs.mupd8.compression;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import com.walmartlabs.mupd8.Constants;

public class GZIPCompression implements CompressionService {

	@Override
	public byte[] compress(byte[] uncompressed) throws Exception {
		ByteArrayOutputStream bos  = new ByteArrayOutputStream(Constants.SLATE_CAPACITY);
		GZIPOutputStream gzos = new GZIPOutputStream(bos, Constants.COMPRESSED_CAPACITY);
		assert(uncompressed.length < Constants.SLATE_CAPACITY);
		gzos.write(uncompressed, 0, uncompressed.length);
		gzos.flush();
		gzos.close();
		bos.flush();
		byte[] compressed = bos.toByteArray();
		bos.close();
		return compressed;
	}
	
	@Override
	public byte[] uncompress(byte[] compressed) throws Exception {
		ByteArrayOutputStream buffer = new ByteArrayOutputStream(Constants.SLATE_CAPACITY);
		GZIPInputStream gzis = new GZIPInputStream(new ByteArrayInputStream(compressed));
		byte[] bbuf = new byte[256];
	    while (true) {
	    	int r = gzis.read(bbuf, 0, 256);
	        if (r < 0) {
	          break;
	        }
	        buffer.write(bbuf, 0, r);
	      }
		gzis.close();
		return buffer.toByteArray();
	}
}
