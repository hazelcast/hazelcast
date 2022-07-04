/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.ascii.memcache;

import com.hazelcast.internal.ascii.AbstractTextCommandProcessor;
import com.hazelcast.internal.ascii.TextCommandService;

import static java.lang.System.arraycopy;

@SuppressWarnings("checkstyle:magicnumber")
public abstract class MemcacheCommandProcessor<T> extends AbstractTextCommandProcessor<T> {

    public static final String MAP_NAME_PREFIX = "hz_memcache_";
    public static final String DEFAULT_MAP_NAME = "hz_memcache_default";

    protected MemcacheCommandProcessor(TextCommandService textCommandService) {
        super(textCommandService);
    }

    public static byte[] concatenate(byte[] a, byte[] b) {
        byte[] c = new byte[a.length + b.length];
        arraycopy(a, 0, c, 0, a.length);
        arraycopy(b, 0, c, a.length, b.length);
        return c;
    }
}
