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

import com.hazelcast.client.KeyBasedClientRequest;
import com.hazelcast.client.SecureRequest;
import com.hazelcast.concurrent.countdownlatch.CountDownLatchService;
import com.hazelcast.concurrent.countdownlatch.operations.SetCountOperation;
import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;
import com.hazelcast.security.permission.ActionConstants;
import com.hazelcast.security.permission.CountDownLatchPermission;
import com.hazelcast.spi.Operation;

import java.io.IOException;
import java.security.Permission;

public final class SetCountRequest extends KeyBasedClientRequest
        implements Portable, SecureRequest {

    private String name;
    private int count;

    public SetCountRequest() {
    }

    public SetCountRequest(String name, int count) {
        this.name = name;
        this.count = count;
    }

    @Override
    protected Object getKey() {
        return name;
    }

    @Override
    protected Operation prepareOperation() {
        return new SetCountOperation(name, count);
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
        return CountDownLatchPortableHook.SET_COUNT;
    }

    @Override
    public void write(PortableWriter writer) throws IOException {
        writer.writeUTF("name", name);
        writer.writeInt("count", count);
    }

    @Override
    public void read(PortableReader reader) throws IOException {
        name = reader.readUTF("name");
        count = reader.readInt("count");
    }

    @Override
    public Permission getRequiredPermission() {
        return new CountDownLatchPermission(name, ActionConstants.ACTION_MODIFY);
    }

    @Override
    public String getMethodName() {
        return "trySetCount";
    }

    @Override
    public Object[] getParameters() {
        return new Object[]{count};
    }
}
