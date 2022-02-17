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

package com.hazelcast.cache.impl.operation;

import com.hazelcast.cache.impl.CacheDataSerializerHook;
import com.hazelcast.cache.impl.ICacheRecordStore;
import com.hazelcast.cache.impl.ICacheService;
import com.hazelcast.internal.nio.IOUtil;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.spi.impl.operationservice.ReadonlyOperation;

import javax.cache.expiry.ExpiryPolicy;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import static com.hazelcast.internal.util.SetUtil.createHashSet;

/**
 * Gets all keys from the cache.
 * <p>{@link com.hazelcast.cache.impl.operation.CacheGetAllOperationFactory} creates this operation.</p>
 * <p>Functionality: Filters out the partition keys and calls
 * {@link com.hazelcast.cache.impl.ICacheRecordStore#getAll(java.util.Set, javax.cache.expiry.ExpiryPolicy)}</p>
 */
public class CacheGetAllOperation
        extends PartitionWideCacheOperation
        implements ReadonlyOperation {

    private Set<Data> keys;
    private ExpiryPolicy expiryPolicy;

    public CacheGetAllOperation(String name, Set<Data> keys, ExpiryPolicy expiryPolicy) {
        super(name);
        this.keys = keys;
        this.expiryPolicy = expiryPolicy;
    }

    public CacheGetAllOperation() {
        keys = new HashSet<Data>();
    }

    public void run() {
        ICacheService service = getService();
        ICacheRecordStore cache = service.getOrCreateRecordStore(name, getPartitionId());

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
    public int getClassId() {
        return CacheDataSerializerHook.GET_ALL;
    }

    @Override
    protected void toString(StringBuilder sb) {
        super.toString(sb);

        sb.append(", keys=").append(keys.toString());
        sb.append(", expiryPolicy=").append(expiryPolicy);
    }

    @Override
    protected void writeInternal(ObjectDataOutput out)
            throws IOException {
        //TODO not used validate and remove !!
        super.writeInternal(out);
        out.writeObject(expiryPolicy);
        if (keys == null) {
            out.writeInt(-1);
        } else {
            out.writeInt(keys.size());
            for (Data key : keys) {
                IOUtil.writeData(out, key);
            }
        }
    }

    @Override
    protected void readInternal(ObjectDataInput in)
            throws IOException {
        //TODO not used validate and remove !!
        super.readInternal(in);
        expiryPolicy = in.readObject();
        int size = in.readInt();
        if (size > -1) {
            keys = createHashSet(size);
            for (int i = 0; i < size; i++) {
                Data key = IOUtil.readData(in);
                keys.add(key);
            }
        }
    }
}
