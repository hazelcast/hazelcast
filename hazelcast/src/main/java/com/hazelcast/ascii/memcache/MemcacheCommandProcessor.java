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

    public static final String MAP_NAME_PRECEDER = "hz_memcache_";
    public static final String DEFAULT_MAP_NAME = "hz_memcache_default";

    protected MemcacheCommandProcessor(TextCommandService textCommandService) {
        super(textCommandService);
    }

    public static byte[] longToByteArray(long v) {
        long paramV = v;
        int len = (int) (paramV / 256) + 1;
        final byte[] bytes = new byte[len];
        for (int i = len - 1; i >= 0; i--) {
            final long t = paramV % 256;
            bytes[i] = t < 128 ? (byte) t : (byte) (t - 256);
            paramV = (paramV - t) / 256;
        }
        return bytes;
    }

    public static int byteArrayToLong(byte[] v) {
        if (v.length > 8) {
            return -1;
        }
        int r = 0;
        for (int i = 0; i < v.length; i++) {
            int t = (int) v[i];
            t = t >= 0 ? t : t + 256;
            r = r * 256 + t;
        }
        return r;
    }


}
