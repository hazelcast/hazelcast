/*
 * Copyright (c) 2008-2012, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.collection.CollectionCompareAndRemoveOperation;
import com.hazelcast.collection.CollectionPortableHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;
import com.hazelcast.spi.Operation;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/**
 * @ali 9/4/13
 */
public class CollectionCompareAndRemoveRequest extends CollectionRequest {

    private Set<Data> valueSet;

    private boolean retain;

    public CollectionCompareAndRemoveRequest() {
    }

    public CollectionCompareAndRemoveRequest(String name, Set<Data> valueSet, boolean retain) {
        super(name);
        this.valueSet= valueSet;
        this.retain = retain;
    }

    protected Operation prepareOperation() {
        return new CollectionCompareAndRemoveOperation(name, retain, valueSet);
    }

    public int getClassId() {
        return CollectionPortableHook.COLLECTION_COMPARE_AND_REMOVE;
    }

    public void writePortable(PortableWriter writer) throws IOException {
        super.writePortable(writer);
        writer.writeBoolean("r", retain);
        final ObjectDataOutput out = writer.getRawDataOutput();
        out.writeInt(valueSet.size());
        for (Data value : valueSet) {
            value.writeData(out);
        }
    }

    public void readPortable(PortableReader reader) throws IOException {
        super.readPortable(reader);
        retain = reader.readBoolean("r");
        final ObjectDataInput in = reader.getRawDataInput();
        final int size = in.readInt();
        valueSet = new HashSet<Data>(size);
        for (int i = 0; i<size; i++){
            final Data value = new Data();
            value.readData(in);
            valueSet.add(value);
        }
    }
}
