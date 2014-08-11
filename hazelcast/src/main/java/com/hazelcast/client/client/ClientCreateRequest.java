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

package com.hazelcast.client.client;

import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;
import com.hazelcast.security.permission.ActionConstants;
import com.hazelcast.spi.ProxyService;

import java.io.IOException;
import java.security.Permission;

public class ClientCreateRequest extends CallableClientRequest implements Portable, RetryableRequest, SecureRequest {

    private String name;

    private String serviceName;

    public ClientCreateRequest() {
    }

    public ClientCreateRequest(String name, String serviceName) {
        this.name = name;
        this.serviceName = serviceName;
    }

    @Override
    public Object call() throws Exception {
        ProxyService proxyService = clientEngine.getProxyService();
        proxyService.initializeDistributedObject(serviceName, name);
        return null;
    }

    @Override
    public String getServiceName() {
        return serviceName;
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
    public void write(PortableWriter writer) throws IOException {
        writer.writeUTF("n", name);
        writer.writeUTF("s", serviceName);
    }

    @Override
    public void read(PortableReader reader) throws IOException {
        name = reader.readUTF("n");
        serviceName = reader.readUTF("s");
    }

    @Override
    public Permission getRequiredPermission() {
        return ActionConstants.getPermission(name, serviceName, ActionConstants.ACTION_CREATE);
    }
}
