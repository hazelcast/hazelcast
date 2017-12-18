/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.replicatedmap.impl.ReplicatedMapService;
import com.hazelcast.replicatedmap.impl.client.ReplicatedMapValueCollection;
import com.hazelcast.replicatedmap.impl.record.ReplicatedRecord;
import com.hazelcast.replicatedmap.impl.record.ReplicatedRecordStore;
import com.hazelcast.spi.ReadonlyOperation;
import com.hazelcast.spi.serialization.SerializationService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;

public class ValuesOperation extends AbstractNamedSerializableOperation implements ReadonlyOperation {

    private String name;
    private transient Object response;

    public ValuesOperation() {
    }

    public ValuesOperation(String name) {
        this.name = name;
    }

    @Override
    public void run() throws Exception {
        ReplicatedMapService service = getService();
        Collection<ReplicatedRecordStore> stores = service.getAllReplicatedRecordStores(name);
        Collection<ReplicatedRecord> values = new ArrayList<ReplicatedRecord>();
        for (ReplicatedRecordStore store : stores) {
            values.addAll(store.values(false));
        }
        Collection<Data> dataValues = new ArrayList<Data>(values.size());
        SerializationService serializationService = getNodeEngine().getSerializationService();
        for (ReplicatedRecord value : values) {
            dataValues.add(serializationService.toData(value.getValue()));
        }
        response = new ReplicatedMapValueCollection(dataValues);
    }

    @Override
    public Object getResponse() {
        return response;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        out.writeUTF(name);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        name = in.readUTF();
    }

    @Override
    public int getId() {
        return ReplicatedMapDataSerializerHook.VALUES;
    }

    @Override
    public String getName() {
        return name;
    }
}
