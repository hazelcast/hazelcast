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

import com.hazelcast.client.impl.client.TargetClientRequest;
import com.hazelcast.executor.impl.DistributedExecutorService;
import com.hazelcast.executor.impl.ExecutorPortableHook;
import com.hazelcast.executor.impl.operations.MemberCallableTaskOperation;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;
import com.hazelcast.security.SecurityContext;
import com.hazelcast.spi.Operation;
import com.hazelcast.util.ConstructorFunction;

import javax.security.auth.Subject;
import java.io.IOException;
import java.security.Permission;
import java.util.concurrent.Callable;

/**
 * This class is used for sending the task to a particular target
 */
public final class TargetCallableRequest extends TargetClientRequest implements RefreshableRequest {

    private String name;
    private String uuid;
    private Callable callable;
    private volatile Address target;
    private ConstructorFunction<Object, Address> targetAddressCreator;

    public TargetCallableRequest() {
    }

    public TargetCallableRequest(String name, String uuid, Callable callable, Address target) {
        this(name, uuid, callable, target, null);
    }

    public TargetCallableRequest(String name, String uuid, Callable callable,
                                 ConstructorFunction<Object, Address> targetAddressCreator) {
        this(name, uuid, callable, null, targetAddressCreator);
    }

    private TargetCallableRequest(String name, String uuid, Callable callable,
                                  Address target, ConstructorFunction<Object, Address> targetAddressCreator) {
        this.name = name;
        this.uuid = uuid;
        this.callable = callable;
        this.target = targetAddressCreator == null ? target : targetAddressCreator.createNew(null);
        this.targetAddressCreator = targetAddressCreator;
    }

    @SuppressWarnings("unchecked")
    @Override
    protected Operation prepareOperation() {
        SecurityContext securityContext = getClientEngine().getSecurityContext();
        if (securityContext != null) {
            Subject subject = getEndpoint().getSubject();
            callable = securityContext.createSecureCallable(subject, callable);
        }
        Data callableData = serializationService.toData(callable);
        return new MemberCallableTaskOperation(name, uuid, callableData);
    }

    @Override
    public Address getTarget() {
        return target;
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
        return ExecutorPortableHook.TARGET_CALLABLE_REQUEST;
    }

    @Override
    public void write(PortableWriter writer) throws IOException {
        writer.writeUTF("n", name);
        writer.writeUTF("u", uuid);
        ObjectDataOutput rawDataOutput = writer.getRawDataOutput();
        rawDataOutput.writeObject(callable);
        target.writeData(rawDataOutput);
    }

    @Override
    public void read(PortableReader reader) throws IOException {
        name = reader.readUTF("n");
        uuid = reader.readUTF("u");
        ObjectDataInput rawDataInput = reader.getRawDataInput();
        callable = rawDataInput.readObject();
        target = new Address();
        target.readData(rawDataInput);
    }

    @Override
    public Permission getRequiredPermission() {
        return null;
    }

    @Override
    public void refresh() {
        if (targetAddressCreator == null) {
            return;
        }
        target = targetAddressCreator.createNew(null);
    }
}
