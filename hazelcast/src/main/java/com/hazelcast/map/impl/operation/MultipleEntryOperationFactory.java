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

package com.hazelcast.map.impl.operation;

import com.hazelcast.internal.nio.IOUtil;
import com.hazelcast.map.EntryProcessor;
import com.hazelcast.map.impl.MapDataSerializerHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.spi.impl.operationservice.Operation;

import java.io.IOException;
import java.util.Set;

import static com.hazelcast.internal.util.SetUtil.createHashSet;

public class MultipleEntryOperationFactory extends AbstractMapOperationFactory {

    private String name;
    private Set<Data> keys;
    private EntryProcessor entryProcessor;

    public MultipleEntryOperationFactory() {
    }

    public MultipleEntryOperationFactory(String name, Set<Data> keys, EntryProcessor entryProcessor) {
        this.name = name;
        this.keys = keys;
        this.entryProcessor = entryProcessor;
    }

    @Override
    public Operation createOperation() {
        return new MultipleEntryOperation(name, keys, entryProcessor);
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeString(name);
        out.writeInt(keys.size());
        for (Data key : keys) {
            IOUtil.writeData(out, key);
        }
        out.writeObject(entryProcessor);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        this.name = in.readString();
        int size = in.readInt();
        this.keys = createHashSet(size);
        for (int i = 0; i < size; i++) {
            Data key = IOUtil.readData(in);
            keys.add(key);
        }
        this.entryProcessor = in.readObject();
    }

    @Override
    public int getClassId() {
        return MapDataSerializerHook.MULTIPLE_ENTRY_FACTORY;
    }
}
