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

package com.hazelcast.concurrent.atomiclong.client;

import com.hazelcast.client.PartitionClientRequest;
import com.hazelcast.client.SecureRequest;
import com.hazelcast.concurrent.atomiclong.AtomicLongPortableHook;
import com.hazelcast.concurrent.atomiclong.AtomicLongService;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;
import com.hazelcast.security.permission.ActionConstants;
import com.hazelcast.security.permission.AtomicLongPermission;

import java.io.IOException;
import java.security.Permission;

/**
 * @author ali 5/13/13
 */
public abstract class AtomicLongRequest extends PartitionClientRequest implements Portable, SecureRequest {

    String name;

    long delta;

    protected AtomicLongRequest() {
    }

    protected AtomicLongRequest(String name, long delta) {
        this.name = name;
        this.delta = delta;
    }

    protected int getPartition() {
        Data key = getClientEngine().getSerializationService().toData(name);
        return getClientEngine().getPartitionService().getPartitionId(key);
    }

    protected int getReplicaIndex() {
        return 0;
    }

    public String getServiceName() {
        return AtomicLongService.SERVICE_NAME;
    }

    public int getFactoryId() {
        return AtomicLongPortableHook.F_ID;
    }

    public void writePortable(PortableWriter writer) throws IOException {
        writer.writeUTF("n",name);
        writer.writeLong("d",delta);
    }

    public void readPortable(PortableReader reader) throws IOException {
        name = reader.readUTF("n");
        delta = reader.readLong("d");
    }

    public Permission getRequiredPermission() {
        return new AtomicLongPermission(name, ActionConstants.ACTION_MODIFY);
    }
}
