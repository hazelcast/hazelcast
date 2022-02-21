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
import com.hazelcast.internal.nio.IOUtil;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.spi.impl.operationservice.ReadonlyOperation;

import java.io.IOException;
import java.util.Set;

import static com.hazelcast.internal.util.SetUtil.createHashSet;

public class CollectionContainsOperation extends CollectionOperation implements ReadonlyOperation {

    private Set<Data> valueSet;

    public CollectionContainsOperation() {
    }

    public CollectionContainsOperation(String name, Set<Data> valueSet) {
        super(name);
        this.valueSet = valueSet;
    }

    @Override
    public void run() throws Exception {
        CollectionContainer collectionContainer = getOrCreateContainer();
        response = collectionContainer.contains(valueSet);
    }

    @Override
    public int getClassId() {
        return CollectionDataSerializerHook.COLLECTION_CONTAINS;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeInt(valueSet.size());
        for (Data value : valueSet) {
            IOUtil.writeData(out, value);
        }
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        final int size = in.readInt();
        valueSet = createHashSet(size);
        for (int i = 0; i < size; i++) {
            Data value = IOUtil.readData(in);
            valueSet.add(value);
        }
    }
}
