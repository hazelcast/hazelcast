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

package com.hazelcast.concurrent.semaphore.client;

import com.hazelcast.client.PartitionClientRequest;
import com.hazelcast.concurrent.semaphore.SemaphorePortableHook;
import com.hazelcast.concurrent.semaphore.SemaphoreService;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;

import java.io.IOException;

/**
 * @ali 5/13/13
 */
public abstract class SemaphoreRequest extends PartitionClientRequest implements Portable {

    String name;

    int permitCount;

    protected SemaphoreRequest() {
    }

    protected SemaphoreRequest(String name, int permitCount) {
        this.name = name;
        this.permitCount = permitCount;
    }

    protected int getPartition() {
        Data key = getClientEngine().getSerializationService().toData(name);
        return getClientEngine().getPartitionService().getPartitionId(key);
    }

    protected int getReplicaIndex() {
        return 0;
    }

    public String getServiceName() {
        return SemaphoreService.SERVICE_NAME;
    }

    public int getFactoryId() {
        return SemaphorePortableHook.F_ID;
    }

    public void writePortable(PortableWriter writer) throws IOException {
        writer.writeUTF("n",name);
        writer.writeInt("p",permitCount);
    }

    public void readPortable(PortableReader reader) throws IOException {
        name = reader.readUTF("n");
        permitCount = reader.readInt("p");
    }
}
