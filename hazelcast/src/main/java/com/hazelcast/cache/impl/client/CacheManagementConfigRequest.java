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

package com.hazelcast.cache.impl.client;

import com.hazelcast.cache.impl.CachePortableHook;
import com.hazelcast.cache.impl.CacheService;
import com.hazelcast.cache.impl.operation.CacheManagementConfigOperation;
import com.hazelcast.client.impl.client.TargetClientRequest;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;
import com.hazelcast.spi.Operation;

import java.io.IOException;
import java.security.Permission;

/**
 * This client request  specifically calls {@link CacheManagementConfigOperation} on the server side.
 * @see com.hazelcast.cache.impl.operation.CacheManagementConfigOperation
 */
public class CacheManagementConfigRequest
        extends TargetClientRequest {

    private String name;
    private boolean isStat;
    private boolean enabled;

    private Address target;

    public CacheManagementConfigRequest() {
    }

    public CacheManagementConfigRequest(String name, boolean isStat, boolean enabled, Address target) {
        this.name = name;
        this.isStat = isStat;
        this.enabled = enabled;
        this.target = target;
    }

    @Override
    public int getFactoryId() {
        return CachePortableHook.F_ID;
    }

    public int getClassId() {
        return CachePortableHook.MANAGEMENT_CONFIG;
    }

    @Override
    protected Operation prepareOperation() {
        return new CacheManagementConfigOperation(name, isStat, enabled);
    }

    @Override
    public Address getTarget() {
        return target;
    }

    @Override
    public String getServiceName() {
        return CacheService.SERVICE_NAME;
    }

    public void write(PortableWriter writer)
            throws IOException {
        writer.writeUTF("n", name);
        writer.writeBoolean("s", isStat);
        writer.writeBoolean("e", enabled);
        ObjectDataOutput rawDataOutput = writer.getRawDataOutput();
        target.writeData(rawDataOutput);
    }

    public void read(PortableReader reader)
            throws IOException {
        name = reader.readUTF("n");
        isStat = reader.readBoolean("s");
        enabled = reader.readBoolean("e");
        ObjectDataInput rawDataInput = reader.getRawDataInput();
        target = new Address();
        target.readData(rawDataInput);
    }

    @Override
    public Permission getRequiredPermission() {
        return null;
    }
}
