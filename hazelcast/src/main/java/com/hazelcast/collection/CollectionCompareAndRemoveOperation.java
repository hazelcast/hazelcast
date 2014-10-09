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

package com.hazelcast.collection;

import com.hazelcast.core.ItemEventType;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.Operation;
import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class CollectionCompareAndRemoveOperation extends CollectionBackupAwareOperation {

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
    public int getId() {
        return CollectionDataSerializerHook.COLLECTION_COMPARE_AND_REMOVE;
    }

    @Override
    public void beforeRun() throws Exception {
    }

    @Override
    public void run() throws Exception {
        itemIdMap = getOrCreateContainer().compareAndRemove(retain, valueSet);
        response = !itemIdMap.isEmpty();
    }

    @Override
    public void afterRun() throws Exception {
        for (Data value : itemIdMap.values()) {
            publishEvent(ItemEventType.REMOVED, value);
        }
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeBoolean(retain);
        out.writeInt(valueSet.size());
        for (Data value : valueSet) {
            out.writeData(value);
        }
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        retain = in.readBoolean();
        final int size = in.readInt();
        valueSet = new HashSet<Data>(size);
        for (int i = 0; i < size; i++) {
            Data value = in.readData();
            valueSet.add(value);
        }
    }
}
