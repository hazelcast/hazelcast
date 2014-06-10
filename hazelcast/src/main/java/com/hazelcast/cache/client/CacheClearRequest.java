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

package com.hazelcast.cache.client;

import com.hazelcast.cache.operation.CacheClearOperationFactory;
import com.hazelcast.cache.CachePortableHook;
import com.hazelcast.cache.CacheService;
import com.hazelcast.client.AllPartitionsClientRequest;
import com.hazelcast.client.RetryableRequest;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;
import com.hazelcast.spi.OperationFactory;

import java.io.IOException;
import java.security.Permission;
import java.util.Map;

public class CacheClearRequest extends AllPartitionsClientRequest implements RetryableRequest {

    private String name;

    public CacheClearRequest() {
    }

    public CacheClearRequest(String name) {
        this.name = name;
    }

    public String getServiceName() {
        return CacheService.SERVICE_NAME;
    }

    @Override
    public int getFactoryId() {
        return CachePortableHook.F_ID;
    }

    public int getClassId() {
        return CachePortableHook.CLEAR;
    }

    public void write(PortableWriter writer) throws IOException {
        writer.writeUTF("n", name);
    }

    public void read(PortableReader reader) throws IOException {
        name = reader.readUTF("n");
    }

    @Override
    protected OperationFactory createOperationFactory() {
        return new CacheClearOperationFactory(name);
    }

    @Override
    protected Object reduce(Map<Integer, Object> map) {
        return null;
    }

    @Override
    public Permission getRequiredPermission() {
        return null;
    }
}
