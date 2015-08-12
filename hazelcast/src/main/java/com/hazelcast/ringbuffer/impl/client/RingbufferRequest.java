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

package com.hazelcast.ringbuffer.impl.client;

import com.hazelcast.client.impl.client.PartitionClientRequest;
import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;
import com.hazelcast.partition.strategy.StringPartitioningStrategy;
import com.hazelcast.ringbuffer.impl.RingbufferService;

import java.io.IOException;

public abstract class RingbufferRequest
        extends PartitionClientRequest implements Portable {

    protected String name;

    protected RingbufferRequest() {
    }

    protected RingbufferRequest(String name) {
        this.name = name;
    }

    @Override
    protected int getPartition() {
        final String partitionKey = StringPartitioningStrategy.getPartitionKey(name);
        return getClientEngine().getPartitionService().getPartitionId(partitionKey);
    }

    @Override
    public String getServiceName() {
        return RingbufferService.SERVICE_NAME;
    }

    @Override
    public int getFactoryId() {
        return RingbufferPortableHook.F_ID;
    }

    @Override
    public void write(PortableWriter writer) throws IOException {
        writer.writeUTF("n", name);
    }

    @Override
    public void read(PortableReader reader) throws IOException {
        name = reader.readUTF("n");
    }

    @Override
    public String getDistributedObjectName() {
        return name;
    }
}
