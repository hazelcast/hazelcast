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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class CollectionAddAllOperation extends CollectionBackupAwareOperation {

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
    public int getId() {
        return CollectionDataSerializerHook.COLLECTION_ADD_ALL;
    }

    @Override
    public void beforeRun() throws Exception {
    }

    @Override
    public void run() throws Exception {
        if (!hasEnoughCapacity(valueList.size())) {
            response = false;
            return;
        }

        valueMap = getOrCreateContainer().addAll(valueList);
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
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeInt(valueList.size());
        for (Data value : valueList) {
            value.writeData(out);
        }
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        final int size = in.readInt();
        valueList = new ArrayList<Data>(size);
        for (int i = 0; i < size; i++) {
            final Data value = new Data();
            value.readData(in);
            valueList.add(value);
        }
    }
}
