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

package com.hazelcast.queue.client;

import com.hazelcast.client.PartitionClientRequest;
import com.hazelcast.client.SecureRequest;
import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;
import com.hazelcast.partition.strategy.StringPartitioningStrategy;
import com.hazelcast.queue.QueuePortableHook;
import com.hazelcast.queue.QueueService;
import com.hazelcast.security.permission.ActionConstants;
import com.hazelcast.security.permission.QueuePermission;

import java.io.IOException;
import java.security.Permission;

/**
 * @author ali 5/8/13
 */
public abstract class QueueRequest extends PartitionClientRequest implements Portable, SecureRequest {

    protected String name;

    protected long timeoutMillis;

    protected QueueRequest() {
    }

    protected QueueRequest(String name) {
        this.name = name;
    }

    protected QueueRequest(String name, long timeoutMillis) {
        this.name = name;
        this.timeoutMillis = timeoutMillis;
    }

    protected int getPartition() {
        final String partitionKey = StringPartitioningStrategy.getPartitionKey(name);
        return getClientEngine().getPartitionService().getPartitionId(partitionKey);
    }

    public String getServiceName() {
        return QueueService.SERVICE_NAME;
    }

    public int getFactoryId() {
        return QueuePortableHook.F_ID;
    }

    public void write(PortableWriter writer) throws IOException {
        writer.writeUTF("n",name);
        writer.writeLong("t",timeoutMillis);
    }

    public void read(PortableReader reader) throws IOException {
        name = reader.readUTF("n");
        timeoutMillis = reader.readLong("t");
    }

    public Permission getRequiredPermission() {
        return new QueuePermission(name, ActionConstants.ACTION_READ);
    }
}
