/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.replicatedmap.impl.client.ReplicatedMapEntries;
import com.hazelcast.replicatedmap.impl.record.ReplicatedRecord;
import com.hazelcast.replicatedmap.impl.record.ReplicatedRecordStore;
import com.hazelcast.spi.ReadonlyOperation;
import com.hazelcast.spi.serialization.SerializationService;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

public class EntrySetOperation extends AbstractNamedSerializableOperation implements ReadonlyOperation {

    private String name;

    private transient Object response;

    public EntrySetOperation() {
    }

    public EntrySetOperation(String name) {
        this.name = name;
    }

    @Override
    public void run() throws Exception {
        ReplicatedMapService service = getService();
        Collection<ReplicatedRecordStore> stores = service.getAllReplicatedRecordStores(name);
        List<Map.Entry<Object, ReplicatedRecord>> entries = new ArrayList<Map.Entry<Object, ReplicatedRecord>>();
        for (ReplicatedRecordStore store : stores) {
            entries.addAll(store.entrySet(false));
        }
        ArrayList<Map.Entry<Data, Data>> dataEntries = new ArrayList<Map.Entry<Data, Data>>(entries.size());
        SerializationService serializationService = getNodeEngine().getSerializationService();
        for (Map.Entry<Object, ReplicatedRecord> entry : entries) {
            Data key = serializationService.toData(entry.getKey());
            Data value = serializationService.toData(entry.getValue().getValue());
            dataEntries.add(new AbstractMap.SimpleImmutableEntry<Data, Data>(key, value));
        }
        response = new ReplicatedMapEntries(dataEntries);
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
        return ReplicatedMapDataSerializerHook.ENTRY_SET;
    }

    @Override
    public String getName() {
        return name;
    }
}
