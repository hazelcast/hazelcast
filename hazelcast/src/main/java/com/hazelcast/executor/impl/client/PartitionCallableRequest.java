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

package com.hazelcast.executor.impl.client;

import com.hazelcast.client.impl.client.PartitionClientRequest;
import com.hazelcast.executor.impl.CallableTaskOperation;
import com.hazelcast.executor.impl.DistributedExecutorService;
import com.hazelcast.executor.impl.ExecutorPortableHook;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;
import com.hazelcast.security.SecurityContext;
import com.hazelcast.spi.Operation;
import com.hazelcast.util.ConstructorFunction;

import java.io.IOException;
import java.security.Permission;
import java.util.concurrent.Callable;

/**
 * This class is used for sending the task to a particular partition
 */
public class PartitionCallableRequest extends PartitionClientRequest implements RefreshableRequest {

    private String name;
    private String uuid;
    private Callable callable;
    private int partitionId;
    private Address target;
    private ConstructorFunction<Object, Address> targetCreator;

    public PartitionCallableRequest() {
    }

    public PartitionCallableRequest(String name, String uuid, Callable callable, int partitionId) {
        this(name, uuid, callable, partitionId, null);
    }

    public PartitionCallableRequest(String name, String uuid, Callable callable, int partitionId,
                                    ConstructorFunction<Object, Address> targetCreator) {
        this.name = name;
        this.uuid = uuid;
        this.callable = callable;
        this.partitionId = partitionId;
        this.targetCreator = targetCreator;
        this.target = targetCreator != null ? targetCreator.createNew(null) : null;
    }

    @Override
    public Address getTarget() {
        return target;
    }

    @Override
    protected Operation prepareOperation() {
        SecurityContext securityContext = getClientEngine().getSecurityContext();
        if (securityContext != null) {
            callable = securityContext.createSecureCallable(getEndpoint().getSubject(), callable);
        }
        return new CallableTaskOperation(name, uuid, callable);
    }

    @Override
    protected int getPartition() {
        return partitionId;
    }

    @Override
    public String getServiceName() {
        return DistributedExecutorService.SERVICE_NAME;
    }

    @Override
    public int getFactoryId() {
        return ExecutorPortableHook.F_ID;
    }

    @Override
    public int getClassId() {
        return ExecutorPortableHook.PARTITION_CALLABLE_REQUEST;
    }

    @Override
    public Permission getRequiredPermission() {
        return null;
    }

    @Override
    public void write(PortableWriter writer) throws IOException {
        writer.writeUTF("n", name);
        writer.writeUTF("u", uuid);
        writer.writeInt("p", partitionId);
        ObjectDataOutput rawDataOutput = writer.getRawDataOutput();
        rawDataOutput.writeObject(callable);
        final boolean isTargetNull = target == null;
        rawDataOutput.writeBoolean(isTargetNull);
        if (!isTargetNull) {
            target.writeData(rawDataOutput);
        }
    }

    @Override
    public void read(PortableReader reader) throws IOException {
        name = reader.readUTF("n");
        uuid = reader.readUTF("u");
        partitionId = reader.readInt("p");
        ObjectDataInput rawDataInput = reader.getRawDataInput();
        callable = rawDataInput.readObject();
        final boolean isTargetNull = rawDataInput.readBoolean();
        if (!isTargetNull) {
            target = new Address();
            target.readData(rawDataInput);
        }
    }

    @Override
    public String toString() {
        return "PartitionCallableRequest{"
                + "name='" + name + '\''
                + ", uuid='" + uuid + '\''
                + ", callable=" + callable
                + ", partitionId=" + partitionId
                + '}';
    }

    @Override
    public void refresh() {
        if (target == null) {
            return;
        }
        target = targetCreator.createNew(null);
    }
}
