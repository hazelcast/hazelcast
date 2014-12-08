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

import com.hazelcast.cache.impl.CacheOperationProvider;
import com.hazelcast.cache.impl.CachePortableHook;
import com.hazelcast.cache.impl.CacheService;
import com.hazelcast.cache.impl.operation.CacheLoadAllOperationFactory;
import com.hazelcast.client.impl.client.RetryableRequest;
import com.hazelcast.client.impl.client.SecureRequest;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;
import com.hazelcast.spi.OperationFactory;

import java.io.IOException;
import java.security.Permission;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * This client request  specifically calls {@link CacheLoadAllOperationFactory} on the server side.
 *
 * @see com.hazelcast.cache.impl.operation.CacheLoadAllOperationFactory
 */
public class CacheLoadAllRequest
        extends AbstractCacheAllPartitionsRequest
        implements Portable, RetryableRequest, SecureRequest {

    private Set<Data> keys = new HashSet<Data>();
    private boolean replaceExistingValues;

    public CacheLoadAllRequest() {
    }

    public CacheLoadAllRequest(String name, Set<Data> keys, boolean replaceExistingValues) {
        super(name);
        this.keys = keys;
        this.replaceExistingValues = replaceExistingValues;
    }

    public int getFactoryId() {
        return CachePortableHook.F_ID;
    }

    public int getClassId() {
        return CachePortableHook.LOAD_ALL;
    }

    @Override
    protected OperationFactory createOperationFactory() {
        CacheOperationProvider operationProvider = getOperationProvider();
        return operationProvider.createLoadAllOperationFactory(keys, replaceExistingValues);
    }

    @Override
    protected Object reduce(Map<Integer, Object> map) {
        return map;
    }

    public String getServiceName() {
        return CacheService.SERVICE_NAME;
    }

    public void write(PortableWriter writer)
            throws IOException {
        super.write(writer);
        writer.writeBoolean("r", replaceExistingValues);
        writer.writeInt("size", keys.size());
        if (!keys.isEmpty()) {
            ObjectDataOutput output = writer.getRawDataOutput();
            for (Data key : keys) {
                output.writeData(key);
            }
        }
    }

    public void read(PortableReader reader)
            throws IOException {
        super.read(reader);
        replaceExistingValues = reader.readBoolean("r");
        int size = reader.readInt("size");
        if (size > 0) {
            ObjectDataInput input = reader.getRawDataInput();
            for (int i = 0; i < size; i++) {
                Data key = input.readData();
                keys.add(key);
            }
        }
    }

    public Permission getRequiredPermission() {
        return null;
    }

    @Override
    public String getMethodName() {
        return "getAll";
    }

    @Override
    public Object[] getParameters() {
        return new Object[]{keys};
    }
}
