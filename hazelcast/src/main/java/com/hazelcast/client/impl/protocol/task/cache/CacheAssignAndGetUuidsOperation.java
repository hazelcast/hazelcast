/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.impl.protocol.task.cache;

import com.hazelcast.cache.impl.CacheDataSerializerHook;
import com.hazelcast.cache.impl.CacheEventHandler;
import com.hazelcast.cache.impl.CacheService;
import com.hazelcast.internal.nearcache.impl.invalidation.MetaDataGenerator;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.PartitionAwareOperation;

import java.io.IOException;
import java.util.UUID;

public class CacheAssignAndGetUuidsOperation extends Operation implements PartitionAwareOperation, IdentifiedDataSerializable {

    private UUID partitionUuid;

    @Override
    public int getFactoryId() {
        return CacheDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return CacheDataSerializerHook.CACHE_ASSIGN_AND_GET_UUIDS;
    }

    @Override
    public void run() throws Exception {
        partitionUuid = getMetaDataGenerator().getOrCreateUuid(getPartitionId());
    }

    @Override
    public Object getResponse() {
        return partitionUuid;
    }

    @Override
    public String getServiceName() {
        return CacheService.SERVICE_NAME;
    }

    private MetaDataGenerator getMetaDataGenerator() {
        CacheService service = getService();
        CacheEventHandler cacheEventHandler = service.getCacheEventHandler();
        return cacheEventHandler.getMetaDataGenerator();
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);

        out.writeLong(partitionUuid.getMostSignificantBits());
        out.writeLong(partitionUuid.getLeastSignificantBits());
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);

        partitionUuid = new UUID(in.readLong(), in.readLong());
    }
}
