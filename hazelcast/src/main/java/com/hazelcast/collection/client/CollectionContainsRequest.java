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

package com.hazelcast.collection.client;

import com.hazelcast.collection.CollectionContainsOperation;
import com.hazelcast.collection.CollectionPortableHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;
import com.hazelcast.security.permission.ActionConstants;
import com.hazelcast.spi.Operation;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class CollectionContainsRequest extends CollectionRequest {

    private Set<Data> valueSet;

    public CollectionContainsRequest() {
    }

    public CollectionContainsRequest(String name, Set<Data> valueSet) {
        super(name);
        this.valueSet = valueSet;
    }

    public CollectionContainsRequest(String name, Data value) {
        super(name);
        valueSet = new HashSet<Data>(1);
        valueSet.add(value);
    }

    @Override
    protected Operation prepareOperation() {
        return new CollectionContainsOperation(name, valueSet);
    }

    @Override
    public int getClassId() {
        return CollectionPortableHook.COLLECTION_CONTAINS;
    }

    public void write(PortableWriter writer) throws IOException {
        super.write(writer);
        final ObjectDataOutput out = writer.getRawDataOutput();
        out.writeInt(valueSet.size());
        for (Data value : valueSet) {
            out.writeData(value);
        }
    }

    public void read(PortableReader reader) throws IOException {
        super.read(reader);
        final ObjectDataInput in = reader.getRawDataInput();
        final int size = in.readInt();
        valueSet = new HashSet<Data>(size);
        for (int i = 0; i < size; i++) {
            Data value = in.readData();
            valueSet.add(value);
        }
    }

    @Override
    public String getRequiredAction() {
        return ActionConstants.ACTION_READ;
    }

    @Override
    public String getMethodName() {
        if (valueSet.size() == 1) {
            return "contains";
        }
        return "containsAll";
    }

    @Override
    public Object[] getParameters() {
        if (valueSet.size() == 1) {
            return valueSet.toArray();
        }
        return new Object[] {valueSet};
    }
}
