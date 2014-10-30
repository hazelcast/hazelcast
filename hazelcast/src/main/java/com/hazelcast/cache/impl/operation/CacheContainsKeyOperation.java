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

package com.hazelcast.cache.impl.operation;

import com.hazelcast.cache.impl.CacheDataSerializerHook;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.ReadonlyOperation;

/**
 * Cache contains key operation, determines if the cache contains an entry for the specified key.
 * <p>
 * More formally, returns <tt>true</tt> if and only if the cache contains a
 * mapping for a key <tt>k</tt> such that <tt>key.equals(k)</tt>.
 * (There can be at most one such mapping.)
 */
public class CacheContainsKeyOperation
        extends AbstractCacheOperation
        implements ReadonlyOperation {

    public CacheContainsKeyOperation() {
    }

    public CacheContainsKeyOperation(String name, Data key) {
        super(name, key);
    }

    @Override
    public void run()
            throws Exception {
        response = cache.contains(key);
    }

    @Override
    public int getId() {
        return CacheDataSerializerHook.CONTAINS_KEY;
    }
}
