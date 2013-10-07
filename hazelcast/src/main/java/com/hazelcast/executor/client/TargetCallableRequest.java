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

package com.hazelcast.executor.client;

import com.hazelcast.client.SecureRequest;
import com.hazelcast.client.TargetClientRequest;
import com.hazelcast.executor.CallableTaskOperation;
import com.hazelcast.executor.DistributedExecutorService;
import com.hazelcast.executor.ExecutorDataSerializerHook;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.security.permission.ActionConstants;
import com.hazelcast.security.permission.ExecutorServicePermission;
import com.hazelcast.spi.Operation;

import java.io.IOException;
import java.security.Permission;
import java.util.concurrent.Callable;

/**
 * @author mdogan 5/13/13
 */
public final class TargetCallableRequest extends TargetClientRequest implements IdentifiedDataSerializable, SecureRequest {

    private String name;
    private Callable callable;
    private Address target;

    public TargetCallableRequest() {
    }

    public TargetCallableRequest(String name, Callable callable, Address target) {
        this.name = name;
        this.callable = callable;
        this.target = target;
    }

    @SuppressWarnings("unchecked")
    protected Operation prepareOperation() {
        return new CallableTaskOperation(name, null, callable);
    }

    public Address getTarget() {
        return target;
    }

    public String getServiceName() {
        return DistributedExecutorService.SERVICE_NAME;
    }

    public int getFactoryId() {
        return ExecutorDataSerializerHook.F_ID;
    }

    public int getId() {
        return ExecutorDataSerializerHook.TARGET_CALLABLE_REQUEST;
    }

    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(name);
        out.writeObject(callable);
        target.writeData(out);
    }

    public void readData(ObjectDataInput in) throws IOException {
        name = in.readUTF();
        callable = in.readObject();
        target = new Address();
        target.readData(in);
    }

    public Permission getRequiredPermission() {
        return new ExecutorServicePermission(name, ActionConstants.ACTION_EXECUTE);
    }
}
