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

package com.hazelcast.replicatedmap.impl.operation;

import com.hazelcast.map.impl.MapEntries;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.replicatedmap.impl.ReplicatedMapService;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.operationservice.OperationFactory;

import java.io.IOException;

/**
 * Factory class for {@link PutAllOperation}.
 */
public class PutAllOperationFactory implements OperationFactory {

    private String name;
    private MapEntries entries;

    public PutAllOperationFactory() {
    }

    public PutAllOperationFactory(String name, MapEntries entries) {
        this.name = name;
        this.entries = entries;
    }

    @Override
    public Operation createOperation() {
        return new PutAllOperation(name, entries)
                .setServiceName(ReplicatedMapService.SERVICE_NAME);
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeString(name);
        out.writeObject(entries);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        name = in.readString();
        entries = in.readObject();
    }

    @Override
    public int getFactoryId() {
        return ReplicatedMapDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return ReplicatedMapDataSerializerHook.PUT_ALL_OP_FACTORY;
    }
}
