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
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.PartitionAwareOperation;
import com.hazelcast.spi.impl.AbstractNamedOperation;

/**
 * Abstract cache operation that implements {@link PartitionAwareOperation} serving a base class to provide
 * multi partition operations.
 * @see com.hazelcast.cache.impl.operation.CacheSizeOperation
 * @see com.hazelcast.cache.impl.operation.CacheGetAllOperation
 * @see com.hazelcast.cache.impl.operation.CacheClearOperation
 * @see com.hazelcast.cache.impl.operation.CacheGetConfigOperation
 */
abstract class PartitionWideCacheOperation
        extends AbstractNamedOperation
        implements PartitionAwareOperation, IdentifiedDataSerializable {

    transient Object response;

    protected PartitionWideCacheOperation() {
    }

    protected PartitionWideCacheOperation(String name) {
        super(name);
    }

    @Override
    public boolean returnsResponse() {
        return true;
    }

    @Override
    public Object getResponse() {
        return response;
    }

    @Override
    public int getFactoryId() {
        return CacheDataSerializerHook.F_ID;
    }

}
