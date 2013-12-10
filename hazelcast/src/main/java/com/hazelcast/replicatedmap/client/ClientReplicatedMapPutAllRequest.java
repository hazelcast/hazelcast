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

package com.hazelcast.replicatedmap.client;

import com.hazelcast.map.MapEntrySet;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;
import com.hazelcast.replicatedmap.record.ReplicatedRecordStore;

import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public class ClientReplicatedMapPutAllRequest extends AbstractReplicatedMapClientRequest {

    private ReplicatedMapEntrySet entrySet;

    ClientReplicatedMapPutAllRequest() {
        super(null);
    }

    public ClientReplicatedMapPutAllRequest(String mapName, ReplicatedMapEntrySet entrySet) {
        super(mapName);
        this.entrySet = entrySet;
    }

    @Override
    public Object call() throws Exception {
        ReplicatedRecordStore recordStore = getReplicatedRecordStore();
        Set<Map.Entry> entries = entrySet.getEntrySet();
        for (Map.Entry entry : entries) {
            recordStore.put(entry.getKey(), entry.getValue());
        }
        return null;
    }

    @Override
    public void writePortable(PortableWriter writer) throws IOException {
        super.writePortable(writer);
        entrySet.writePortable(writer);
    }

    @Override
    public void readPortable(PortableReader reader) throws IOException {
        super.readPortable(reader);
        entrySet = new ReplicatedMapEntrySet();
        entrySet.readPortable(reader);
    }

    @Override
    public int getClassId() {
        return ReplicatedMapPortableHook.PUT_ALL;
    }

}
