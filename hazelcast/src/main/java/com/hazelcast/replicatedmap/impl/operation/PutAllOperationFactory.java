/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.replicatedmap.impl.ReplicatedMapService;
import com.hazelcast.replicatedmap.impl.client.ReplicatedMapEntrySet;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationFactory;
import java.io.IOException;

/**
 * Factory class for {@link PutAllOperation}.
 */
public class PutAllOperationFactory implements OperationFactory {

    private String name;
    private ReplicatedMapEntrySet entrySet = new ReplicatedMapEntrySet();

    public PutAllOperationFactory() {
    }

    public PutAllOperationFactory(String name, ReplicatedMapEntrySet entrySet) {
        this.name = name;
        this.entrySet = entrySet;
    }

    @Override
    public Operation createOperation() {
        PutAllOperation putAllOperation = new PutAllOperation(name, entrySet);
        putAllOperation.setServiceName(ReplicatedMapService.SERVICE_NAME);
        return putAllOperation;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(name);
        out.writeObject(entrySet);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        name = in.readUTF();
        entrySet = in.readObject();
    }
}
