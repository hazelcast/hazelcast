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
import com.hazelcast.executor.DistributedExecutorService;
import com.hazelcast.executor.ExecutorPortableHook;
import com.hazelcast.executor.MemberCallableTaskOperation;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;
import com.hazelcast.security.SecurityContext;
import com.hazelcast.spi.Operation;

import java.io.IOException;
import java.util.concurrent.Callable;

/**
 * @author mdogan 5/13/13
 */
public final class TargetCallableRequest extends TargetClientRequest implements Portable {

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
    @Override
    protected Operation prepareOperation() {
        final SecurityContext securityContext = getClientEngine().getSecurityContext();
        if (securityContext != null) {
            callable = securityContext.createSecureCallable(getEndpoint().getSubject(), callable);
        }
        return new MemberCallableTaskOperation(name, null, callable);
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

    public int getClassId() {
        return ExecutorPortableHook.TARGET_CALLABLE_REQUEST;
    }

    public void write(PortableWriter writer) throws IOException {
        writer.writeUTF("n", name);
        final ObjectDataOutput rawDataOutput = writer.getRawDataOutput();
        rawDataOutput.writeObject(callable);
        target.writeData(rawDataOutput);
    }

    public void read(PortableReader reader) throws IOException {
        name = reader.readUTF("n");
        final ObjectDataInput rawDataInput = reader.getRawDataInput();
        callable = rawDataInput.readObject();
        target = new Address();
        target.readData(rawDataInput);
    }


}
