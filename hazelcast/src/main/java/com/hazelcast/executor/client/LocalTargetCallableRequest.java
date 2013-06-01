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

import com.hazelcast.client.TargetClientRequest;
import com.hazelcast.executor.CallableTaskOperation;
import com.hazelcast.executor.DistributedExecutorService;
import com.hazelcast.executor.ExecutorDataSerializerHook;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.Operation;

import java.io.IOException;
import java.util.concurrent.Callable;

/**
 * @ali 5/26/13
 */
public final class LocalTargetCallableRequest extends TargetClientRequest implements IdentifiedDataSerializable {

    private String name;
    private Callable callable;

    public LocalTargetCallableRequest() {
    }

    public LocalTargetCallableRequest(String name, Callable callable) {
        this.name = name;
        this.callable = callable;
    }

    protected Operation prepareOperation() {
        return new CallableTaskOperation(name, callable);
    }

    public Address getTarget() {
        return getClientEngine().getThisAddress();
    }

    public String getServiceName() {
        return DistributedExecutorService.SERVICE_NAME;
    }

    public int getFactoryId() {
        return ExecutorDataSerializerHook.F_ID;
    }

    public int getId() {
        return ExecutorDataSerializerHook.LOCAL_TARGET_CALLABLE_REQUEST;
    }

    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(name);
        out.writeObject(callable);
    }

    public void readData(ObjectDataInput in) throws IOException {
        name = in.readUTF();
        callable = in.readObject();
    }
}
