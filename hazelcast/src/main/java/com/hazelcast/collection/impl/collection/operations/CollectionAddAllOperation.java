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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class CollectionAddAllOperation extends CollectionBackupAwareOperation implements MutatingOperation {

    protected List<Data> valueList;

    protected Map<Long, Data> valueMap;

    public CollectionAddAllOperation() {
    }

    public CollectionAddAllOperation(String name, List<Data> valueList) {
        super(name);
        this.valueList = valueList;
    }

    @Override
    public boolean shouldBackup() {
        return valueMap != null && !valueMap.isEmpty();
    }

    @Override
    public Operation getBackupOperation() {
        return new CollectionAddAllBackupOperation(name, valueMap);
    }

    @Override
    public void run() throws Exception {
        if (!hasEnoughCapacity(valueList.size())) {
            response = false;
            return;
        }

        CollectionContainer collectionContainer = getOrCreateContainer();
        valueMap = collectionContainer.addAll(valueList);
        response = !valueMap.isEmpty();
    }

    @Override
    public void afterRun() throws Exception {
        if (valueMap == null) {
            return;
        }
        for (Data value : valueMap.values()) {
            publishEvent(ItemEventType.ADDED, value);
        }
        super.afterRun();
    }

    @Override
    public int getClassId() {
        return CollectionDataSerializerHook.COLLECTION_ADD_ALL;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeInt(valueList.size());
        for (Data value : valueList) {
            IOUtil.writeData(out, value);
        }
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        final int size = in.readInt();
        valueList = new ArrayList<Data>(size);
        for (int i = 0; i < size; i++) {
            Data value = IOUtil.readData(in);
            valueList.add(value);
        }
    }
}
