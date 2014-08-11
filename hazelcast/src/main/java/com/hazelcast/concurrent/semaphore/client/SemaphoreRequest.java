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

import com.hazelcast.client.ClientEngine;
import com.hazelcast.client.client.PartitionClientRequest;
import com.hazelcast.client.client.SecureRequest;
import com.hazelcast.concurrent.semaphore.SemaphoreService;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;

import java.io.IOException;

public abstract class SemaphoreRequest extends PartitionClientRequest
        implements Portable, SecureRequest {

    protected String name;
    protected int permitCount;

    protected SemaphoreRequest() {
    }

    protected SemaphoreRequest(String name, int permitCount) {
        this.name = name;
        this.permitCount = permitCount;
    }

    @Override
    protected int getPartition() {
        ClientEngine clientEngine = getClientEngine();
        Data key = serializationService.toData(name);
        return clientEngine.getPartitionService().getPartitionId(key);
    }

    @Override
    public String getServiceName() {
        return SemaphoreService.SERVICE_NAME;
    }

    @Override
    public int getFactoryId() {
        return SemaphorePortableHook.F_ID;
    }

    @Override
    public void write(PortableWriter writer) throws IOException {
        writer.writeUTF("n", name);
        writer.writeInt("p", permitCount);
    }

    @Override
    public void read(PortableReader reader) throws IOException {
        name = reader.readUTF("n");
        permitCount = reader.readInt("p");
    }

    @Override
    public String getDistributedObjectName() {
        return name;
    }

    @Override
    public Object[] getParameters() {
        return new Object[]{permitCount};
    }
}
