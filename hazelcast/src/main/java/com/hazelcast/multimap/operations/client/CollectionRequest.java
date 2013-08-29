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

package com.hazelcast.multimap.operations.client;

import com.hazelcast.client.PartitionClientRequest;
import com.hazelcast.multimap.CollectionPortableHook;
import com.hazelcast.multimap.MultiMapService;
import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;

import java.io.IOException;

/**
 * @author ali 5/9/13
 */
public abstract class CollectionRequest extends PartitionClientRequest implements Portable {

    String name;

    protected CollectionRequest() {
    }

    protected CollectionRequest(String name) {
        this.name = name;
    }

    protected int getReplicaIndex() {
        return 0;
    }

    public String getServiceName() {
        return MultiMapService.SERVICE_NAME;
    }

    public Object getObjectId() {
        return name;
    }

    public int getFactoryId() {
        return CollectionPortableHook.F_ID;
    }

    public void writePortable(PortableWriter writer) throws IOException {
        writer.writeUTF("n", name);
    }

    public void readPortable(PortableReader reader) throws IOException {
        name = reader.readUTF("n");
    }
}
