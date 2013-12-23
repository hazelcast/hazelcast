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

import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;
import com.hazelcast.replicatedmap.record.ReplicatedRecordStore;

import java.io.IOException;

/**
 * @author ali 23/12/13
 */
public class ClientReplicatedMapRemoveEntryListenerRequest extends AbstractReplicatedMapClientRequest {

    String registrationId;

    public ClientReplicatedMapRemoveEntryListenerRequest() {
    }

    public ClientReplicatedMapRemoveEntryListenerRequest(String mapName, String registrationId) {
        super(mapName);
        this.registrationId = registrationId;
    }

    public Object call() throws Exception {
        final ReplicatedRecordStore replicatedRecordStore = getReplicatedRecordStore();
        replicatedRecordStore.removeEntryListenerInternal(registrationId);
        return null;
    }

    public int getClassId() {
        return ReplicatedMapPortableHook.REMOVE_LISTENER;
    }

    public void write(PortableWriter writer) throws IOException {
        super.write(writer);
        writer.writeUTF("r", registrationId);
    }

    public void read(PortableReader reader) throws IOException {
        super.read(reader);
        registrationId = reader.readUTF("r");
    }
}
