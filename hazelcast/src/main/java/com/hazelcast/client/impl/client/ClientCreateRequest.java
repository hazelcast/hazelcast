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

package com.hazelcast.client.impl.client;

import com.hazelcast.nio.Address;
import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;
import com.hazelcast.security.permission.ActionConstants;
import com.hazelcast.spi.InvocationBuilder;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.ProxyService;
import com.hazelcast.spi.impl.proxyservice.impl.operations.InitializeDistributedObjectOperation;

import java.io.IOException;
import java.security.Permission;
import java.util.Collection;

public class ClientCreateRequest extends TargetClientRequest implements Portable, SecureRequest {

    private String name;

    private String serviceName;

    private Address target;

    public ClientCreateRequest() {
    }

    public ClientCreateRequest(String name, String serviceName, Address target) {
        this.name = name;
        this.serviceName = serviceName;
        this.target = target;
    }

    @Override
    public int getFactoryId() {
        return ClientPortableHook.ID;
    }

    @Override
    public int getClassId() {
        return ClientPortableHook.CREATE_PROXY;
    }

    @Override
    protected Operation prepareOperation() {
        return new InitializeDistributedObjectOperation(serviceName, name);
    }

    @Override
    protected InvocationBuilder getInvocationBuilder(Operation op) {
        return operationService.createInvocationBuilder(getServiceName(), op, target).setTryCount(1);
    }

    @Override
    public String getServiceName() {
        return serviceName;
    }

    @Override
    public void write(PortableWriter writer) throws IOException {
        writer.writeUTF("n", name);
        writer.writeUTF("s", serviceName);
        target.writeData(writer.getRawDataOutput());
    }

    @Override
    public void read(PortableReader reader) throws IOException {
        name = reader.readUTF("n");
        serviceName = reader.readUTF("s");
        target = new Address();
        target.readData(reader.getRawDataInput());
    }

    @Override
    public Permission getRequiredPermission() {
        ProxyService proxyService = clientEngine.getProxyService();
        Collection<String> distributedObjectNames = proxyService.getDistributedObjectNames(serviceName);
        if (distributedObjectNames.contains(name)) {
            return null;
        }
        return ActionConstants.getPermission(name, serviceName, ActionConstants.ACTION_CREATE);
    }
}
