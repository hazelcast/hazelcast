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

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;
import com.hazelcast.replicatedmap.record.ReplicatedRecord;
import com.hazelcast.replicatedmap.record.ReplicatedRecordStore;

import java.io.IOException;

public class ClientReplicatedMapGetRequest extends AbstractReplicatedMapClientRequest {

    private Object key;

    ClientReplicatedMapGetRequest() {
        super(null);
    }

    public ClientReplicatedMapGetRequest(String mapName, Object key) {
        super(mapName);
        this.key = key;
    }

    @Override
    public Object call() throws Exception {
        ReplicatedRecordStore recordStore = getReplicatedRecordStore();
        ReplicatedRecord record = recordStore.getReplicatedRecord(key);
        return new ReplicatedMapGetResponse(recordStore.unmarshallValue(record.getValue()),
                record.getTtlMillis(), record.getUpdateTime());
    }

    @Override
    public void writePortable(PortableWriter writer) throws IOException {
        super.writePortable(writer);
        ObjectDataOutput out = writer.getRawDataOutput();
        out.writeObject(key);
    }

    @Override
    public void readPortable(PortableReader reader) throws IOException {
        super.readPortable(reader);
        ObjectDataInput in = reader.getRawDataInput();
        key = in.readObject();
    }

    @Override
    public int getClassId() {
        return ReplicatedMapPortableHook.GET;
    }

}
