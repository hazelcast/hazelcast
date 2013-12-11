/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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
 */

package com.hazelcast.ascii.memcache;

import com.hazelcast.ascii.AbstractTextCommandProcessor;
import com.hazelcast.ascii.TextCommandService;

/**
 * User: sancar
 * Date: 3/8/13
 * Time: 5:33 PM
 */
public abstract class MemcacheCommandProcessor<T> extends AbstractTextCommandProcessor<T> {

    public static final String MapNamePreceder = "hz_memcache_";
    public static final String DefaultMapName = "hz_memcache_default";

    public static byte[] longToByteArray(long v) {
        // how many digit
        int len = (int) Math.log10(v) + 1;
        final byte[] bytes = new byte[len];
        for (int i = len - 1; i >= 0; i--) {
            final long t = v % 10;
            // 0 represent as 48 in ascii table
            bytes[i] = (byte) (t + 48);
            v = (v - t) / 10;
        }
        return bytes;
    }

    public static long byteArrayToLong(byte[] v) {
        // max long data has 20 digits in decimal form
        if (v.length > 20) return -1;
        long r = 0;
        for (int i = 0; i < v.length; i++) {
            r = r * 10; // 0 x 10 = 0 (for first run)
            int t = (int) v[i];
            t = t - 48; // 0 represent as 48 in ascii table
            r = r + t;
        }
        return r;
    }

    protected MemcacheCommandProcessor(TextCommandService textCommandService) {
        super(textCommandService);
    }
}
