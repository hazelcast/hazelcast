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
import com.hazelcast.cache.impl.CacheService;
import com.hazelcast.cache.impl.ICacheRecordStore;
import com.hazelcast.spi.ReadonlyOperation;

/**
 * Cache Size Operation
 */
public class CacheSizeOperation
        extends PartitionWideCacheOperation
        implements ReadonlyOperation {

    public CacheSizeOperation() {
    }

    public CacheSizeOperation(String name) {
        super(name);
    }

    @Override
    public void run()
            throws Exception {
        CacheService service = getService();
        ICacheRecordStore cache = service.getCache(name, getPartitionId());
        response = cache != null ? cache.size() : 0;
    }

    @Override
    public int getId() {
        return CacheDataSerializerHook.SIZE;
    }

}
