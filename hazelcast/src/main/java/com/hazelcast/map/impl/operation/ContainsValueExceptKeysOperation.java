/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map.impl.operation;

import com.hazelcast.map.impl.LocalMapStatsProvider;
import com.hazelcast.map.impl.MapDataSerializerHook;
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.PartitionAwareOperation;
import com.hazelcast.spi.ReadonlyOperation;
import com.hazelcast.util.SetUtil;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

/**
 * Checks if a value is in the map. Ignores the keys which are deleted.
 *
 * @since 3.11
 */
public class ContainsValueExceptKeysOperation extends MapOperation implements PartitionAwareOperation, ReadonlyOperation {

    private boolean contains;
    private Object value;
    private Set<Data> deletedKeys;

    public ContainsValueExceptKeysOperation(String name, Object value, Set<Data> deletedKeys) {
        super(name);
        this.value = value;
        this.deletedKeys = deletedKeys;
    }

    public ContainsValueExceptKeysOperation() {
    }

    @Override
    public int getId() {
        return MapDataSerializerHook.CONTAINS_VALUE_EXCEPT_KEYS;
    }

    @Override
    public void run() throws Exception {
        contains = recordStore.containsValue(value, deletedKeys);
        if (mapContainer.getMapConfig().isStatisticsEnabled()) {
            LocalMapStatsProvider localMapStatsProvider = mapServiceContext.getLocalMapStatsProvider();
            localMapStatsProvider.getLocalMapStatsImpl(name).incrementOtherOperations();
        }
    }

    @Override
    public Object getResponse() {
        return contains;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeObject(value);
        out.writeInt(deletedKeys.size());
        for (Data key : deletedKeys) {
            out.writeData(key);
        }
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        value = in.readObject();
        int size = in.readInt();
        deletedKeys = SetUtil.createHashSet(size);
        for (int i = 0; i < size; i++) {
            deletedKeys.add(in.readData());
        }
    }
}
