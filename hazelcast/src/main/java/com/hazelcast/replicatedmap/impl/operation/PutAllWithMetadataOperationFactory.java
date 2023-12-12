/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.internal.serialization.Data;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.replicatedmap.impl.ReplicatedMapService;
import com.hazelcast.replicatedmap.impl.record.ReplicatedMapEntryView;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.operationservice.OperationFactory;

import java.io.IOException;
import java.util.List;

/**
 * Factory class for {@link PutAllWithMetadataOperation}.
 */
public class PutAllWithMetadataOperationFactory implements OperationFactory {

    private String name;
    private List<ReplicatedMapEntryView<Data, Data>> entryViews;

    public PutAllWithMetadataOperationFactory() {
    }

    public PutAllWithMetadataOperationFactory(String name, List<ReplicatedMapEntryView<Data, Data>> entryViews) {
        this.name = name;
        this.entryViews = entryViews;
    }

    @Override
    public Operation createOperation() {
        return new PutAllWithMetadataOperation(name, entryViews)
                .setServiceName(ReplicatedMapService.SERVICE_NAME);
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeString(name);
        out.writeObject(entryViews);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        name = in.readString();
        entryViews = in.readObject();
    }

    @Override
    public int getFactoryId() {
        return ReplicatedMapDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return ReplicatedMapDataSerializerHook.PUT_ALL_WITH_METADATA_OP_FACTORY;
    }
}
