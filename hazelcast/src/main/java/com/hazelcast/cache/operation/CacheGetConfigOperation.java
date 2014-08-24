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

package com.hazelcast.cache.operation;

import com.hazelcast.cache.CacheDataSerializerHook;
import com.hazelcast.cache.CacheService;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.ReadonlyOperation;

import java.io.IOException;

/**
 */
public class CacheGetConfigOperation
        extends PartitionWideCacheOperation
        implements ReadonlyOperation /*implements BackupAwareOperation*/ {

    public CacheGetConfigOperation() {
    }

    public CacheGetConfigOperation(String name) {
        super(name);
    }

    @Override
    public void run()
            throws Exception {
        final CacheService service = getService();
        final CacheConfig cacheConfig = service.getCacheConfig(name);
        response = cacheConfig;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out)
            throws IOException {
        super.writeInternal(out);
    }

    @Override
    protected void readInternal(ObjectDataInput in)
            throws IOException {
        super.readInternal(in);
    }

    @Override
    public int getId() {
        return CacheDataSerializerHook.GET_CONFIG;
    }
}
