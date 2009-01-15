/* 
 * Copyright (c) 2007-2008, Hazel Ltd. All Rights Reserved.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at 
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package com.hazelcast.nio;

import java.nio.ByteBuffer;

public class BufferUtil {

	public static int copy(ByteBuffer src, ByteBuffer dest) {
		int n = Math.min(src.remaining(), dest.remaining());
		int srcPosition = src.position();
		int destPosition = dest.position();

		int ixSrc = srcPosition + src.arrayOffset();
		int ixDest = destPosition + dest.arrayOffset();

		System.arraycopy(src.array(), ixSrc, dest.array(), ixDest, n);
		src.position(srcPosition + n);
		dest.position(destPosition + n);
		return n;
	}

	public static void putBoolean(ByteBuffer bb, boolean value) {
		bb.put((byte) (value ? 1 : 0));
	}

	public static boolean getBoolean(ByteBuffer bb) {
		return bb.get() == 1 ? true : false;
	}

}
