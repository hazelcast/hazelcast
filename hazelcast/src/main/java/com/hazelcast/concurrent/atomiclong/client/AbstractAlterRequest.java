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

import com.hazelcast.client.client.PartitionClientRequest;
import com.hazelcast.client.client.SecureRequest;
import com.hazelcast.concurrent.atomiclong.AtomicLongService;
import com.hazelcast.core.IFunction;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;
import com.hazelcast.security.permission.ActionConstants;
import com.hazelcast.security.permission.AtomicLongPermission;

import java.io.IOException;
import java.security.Permission;

import static com.hazelcast.nio.IOUtil.readNullableData;
import static com.hazelcast.nio.IOUtil.writeNullableData;

public abstract class AbstractAlterRequest extends PartitionClientRequest implements Portable, SecureRequest {

    protected String name;
    protected Data function;

    public AbstractAlterRequest() {
    }

    public AbstractAlterRequest(String name, Data function) {
        this.name = name;
        this.function = function;
    }

    @Override
    protected int getPartition() {
        Data key = serializationService.toData(name);
        return getClientEngine().getPartitionService().getPartitionId(key);
    }

    @Override
    public String getServiceName() {
        return AtomicLongService.SERVICE_NAME;
    }

    @Override
    public int getFactoryId() {
        return AtomicLongPortableHook.F_ID;
    }

    @Override
    public void write(PortableWriter writer) throws IOException {
        writer.writeUTF("n", name);
        ObjectDataOutput out = writer.getRawDataOutput();
        writeNullableData(out, function);
    }

    @Override
    public void read(PortableReader reader) throws IOException {
        name = reader.readUTF("n");
        ObjectDataInput in = reader.getRawDataInput();
        function = readNullableData(in);
    }

    protected IFunction<Long, Long> getFunction() {
        return serializationService.toObject(function);
    }

    @Override
    public Permission getRequiredPermission() {
        return new AtomicLongPermission(name, ActionConstants.ACTION_MODIFY);
    }

    @Override
    public String getDistributedObjectName() {
        return name;
    }

    @Override
    public Object[] getParameters() {
        return new Object[]{function};
    }
}
