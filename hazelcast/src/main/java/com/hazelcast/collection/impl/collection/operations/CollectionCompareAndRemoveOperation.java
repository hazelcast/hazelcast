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

package com.hazelcast.collection.impl.collection.operations;

import com.hazelcast.collection.impl.collection.CollectionContainer;
import com.hazelcast.collection.impl.collection.CollectionDataSerializerHook;
import com.hazelcast.core.ItemEventType;
import com.hazelcast.internal.nio.IOUtil;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.impl.operationservice.MutatingOperation;
import com.hazelcast.spi.impl.operationservice.Operation;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

import static com.hazelcast.internal.util.SetUtil.createHashSet;

public class CollectionCompareAndRemoveOperation extends CollectionBackupAwareOperation implements MutatingOperation {

    private boolean retain;
    private Set<Data> valueSet;
    private Map<Long, Data> itemIdMap;

    public CollectionCompareAndRemoveOperation() {
    }

    public CollectionCompareAndRemoveOperation(String name, boolean retain, Set<Data> valueSet) {
        super(name);
        this.retain = retain;
        this.valueSet = valueSet;
    }

    @Override
    public boolean shouldBackup() {
        return !itemIdMap.isEmpty();
    }

    @Override
    public Operation getBackupOperation() {
        return new CollectionClearBackupOperation(name, itemIdMap.keySet());
    }

    @Override
    public void run() throws Exception {
        CollectionContainer collectionContainer = getOrCreateContainer();
        itemIdMap = collectionContainer.compareAndRemove(retain, valueSet);
        response = !itemIdMap.isEmpty();
    }

    @Override
    public void afterRun() throws Exception {
        for (Data value : itemIdMap.values()) {
            publishEvent(ItemEventType.REMOVED, value);
        }
        super.afterRun();
    }

    @Override
    public int getClassId() {
        return CollectionDataSerializerHook.COLLECTION_COMPARE_AND_REMOVE;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeBoolean(retain);
        out.writeInt(valueSet.size());
        for (Data value : valueSet) {
            IOUtil.writeData(out, value);
        }
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        retain = in.readBoolean();
        final int size = in.readInt();
        valueSet = createHashSet(size);
        for (int i = 0; i < size; i++) {
            Data value = IOUtil.readData(in);
            valueSet.add(value);
        }
    }
}
