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

package com.hazelcast.concurrent.lock.client;

import com.hazelcast.client.KeyBasedClientRequest;
import com.hazelcast.client.SecureRequest;
import com.hazelcast.concurrent.lock.InternalLockNamespace;
import com.hazelcast.concurrent.lock.LockService;
import com.hazelcast.concurrent.lock.operations.GetLockCountOperation;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;
import com.hazelcast.security.permission.ActionConstants;
import com.hazelcast.security.permission.LockPermission;
import com.hazelcast.spi.Operation;

import java.io.IOException;
import java.security.Permission;

public final class GetLockCountRequest extends KeyBasedClientRequest
        implements Portable, SecureRequest {

    private Data key;

    public GetLockCountRequest() {
    }

    public GetLockCountRequest(Data key) {
        this.key = key;
    }

    @Override
    protected Operation prepareOperation() {
        String name = serializationService.toObject(key);
        return new GetLockCountOperation(new InternalLockNamespace(name), key);
    }

    @Override
    protected Object getKey() {
        return key;
    }

    @Override
    public String getServiceName() {
        return LockService.SERVICE_NAME;
    }

    @Override
    public int getFactoryId() {
        return LockPortableHook.FACTORY_ID;
    }

    @Override
    public int getClassId() {
        return LockPortableHook.GET_LOCK_COUNT;
    }

    public void write(PortableWriter writer) throws IOException {
        ObjectDataOutput out = writer.getRawDataOutput();
        key.writeData(out);
    }

    public void read(PortableReader reader) throws IOException {
        ObjectDataInput in = reader.getRawDataInput();
        key = new Data();
        key.readData(in);
    }

    @Override
    public Permission getRequiredPermission() {
        String name = serializationService.toObject(key);
        return new LockPermission(name, ActionConstants.ACTION_READ);
    }
}
