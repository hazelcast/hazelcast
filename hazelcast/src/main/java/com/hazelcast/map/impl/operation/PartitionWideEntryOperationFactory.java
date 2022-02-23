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

import com.hazelcast.map.EntryProcessor;
import com.hazelcast.map.impl.MapDataSerializerHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.impl.operationservice.Operation;

import java.io.IOException;

public class PartitionWideEntryOperationFactory extends AbstractMapOperationFactory {

    private String name;
    private EntryProcessor entryProcessor;

    public PartitionWideEntryOperationFactory() {
    }

    public PartitionWideEntryOperationFactory(String name, EntryProcessor entryProcessor) {
        this.name = name;
        this.entryProcessor = entryProcessor;
    }

    @Override
    public Operation createOperation() {
        return new PartitionWideEntryOperation(name, entryProcessor);
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeString(name);
        out.writeObject(entryProcessor);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        name = in.readString();
        entryProcessor = in.readObject();
    }

    @Override
    public int getClassId() {
        return MapDataSerializerHook.PARTITION_WIDE_ENTRY_FACTORY;
    }
}
