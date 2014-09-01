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
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.ReadonlyOperation;

import javax.cache.expiry.ExpiryPolicy;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/**
 * Get all keys from Cache.
 */
public class CacheGetAllOperation
        extends PartitionWideCacheOperation
        implements ReadonlyOperation {

    private Set<Data> keys = new HashSet<Data>();
    private ExpiryPolicy expiryPolicy;

    public CacheGetAllOperation(String name, Set<Data> keys, ExpiryPolicy expiryPolicy) {
        super(name);
        this.keys = keys;
        this.expiryPolicy = expiryPolicy;
    }

    public CacheGetAllOperation() {
    }

    public void run() {
        CacheService service = getService();
        ICacheRecordStore cache = service.getOrCreateCache(name, getPartitionId());

        int partitionId = getPartitionId();
        Set<Data> partitionKeySet = new HashSet<Data>();
        for (Data key : keys) {
            if (partitionId == getNodeEngine().getPartitionService().getPartitionId(key)) {
                partitionKeySet.add(key);
            }
        }
        response = cache.getAll(partitionKeySet, expiryPolicy);
    }

    @Override
    public int getId() {
        return CacheDataSerializerHook.GET_ALL;
    }

    @Override
    public String toString() {
        return "CacheGetAllOperation{"
                + "keys:" + keys.toString()
                + "expiryPolicy:" + expiryPolicy
                + '}';
    }

    @Override
    protected void writeInternal(ObjectDataOutput out)
            throws IOException {
        super.writeInternal(out);
        out.writeObject(expiryPolicy);
        if (keys == null) {
            out.writeInt(-1);
        } else {
            out.writeInt(keys.size());
            for (Data key : keys) {
                key.writeData(out);
            }
        }
    }

    @Override
    protected void readInternal(ObjectDataInput in)
            throws IOException {
        super.readInternal(in);
        expiryPolicy = in.readObject();
        int size = in.readInt();
        if (size > -1) {
            for (int i = 0; i < size; i++) {
                Data data = new Data();
                data.readData(in);
                keys.add(data);
            }
        }
    }
}
