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

package com.hazelcast.concurrent.countdownlatch.client;

import com.hazelcast.client.impl.client.KeyBasedClientRequest;
import com.hazelcast.client.impl.client.SecureRequest;
import com.hazelcast.concurrent.countdownlatch.CountDownLatchService;
import com.hazelcast.concurrent.countdownlatch.operations.AwaitOperation;
import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;
import com.hazelcast.security.permission.ActionConstants;
import com.hazelcast.security.permission.CountDownLatchPermission;
import com.hazelcast.spi.Operation;

import java.io.IOException;
import java.security.Permission;
import java.util.concurrent.TimeUnit;

public final class AwaitRequest extends KeyBasedClientRequest implements Portable, SecureRequest {

    private String name;
    private long timeout;

    public AwaitRequest() {
    }

    public AwaitRequest(String name, long timeout) {
        this.name = name;
        this.timeout = timeout;
    }

    @Override
    protected Object getKey() {
        return name;
    }

    @Override
    protected Operation prepareOperation() {
        return new AwaitOperation(name, timeout);
    }

    @Override
    public String getServiceName() {
        return CountDownLatchService.SERVICE_NAME;
    }

    @Override
    public int getFactoryId() {
        return CountDownLatchPortableHook.F_ID;
    }

    @Override
    public int getClassId() {
        return CountDownLatchPortableHook.AWAIT;
    }

    @Override
    public void write(PortableWriter writer) throws IOException {
        writer.writeUTF("name", name);
        writer.writeLong("timeout", timeout);
    }

    @Override
    public void read(PortableReader reader) throws IOException {
        name = reader.readUTF("name");
        timeout = reader.readLong("timeout");
    }

    @Override
    public Permission getRequiredPermission() {
        return new CountDownLatchPermission(name, ActionConstants.ACTION_READ);
    }

    @Override
    public String getDistributedObjectName() {
        return name;
    }

    @Override
    public String getMethodName() {
        return "await";
    }

    @Override
    public Object[] getParameters() {
        return new Object[]{timeout, TimeUnit.MILLISECONDS};
    }
}
