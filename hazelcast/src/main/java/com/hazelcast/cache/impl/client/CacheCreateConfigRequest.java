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

package com.hazelcast.cache.impl.client;

import com.hazelcast.cache.impl.CachePortableHook;
import com.hazelcast.cache.impl.CacheService;
import com.hazelcast.cache.impl.operation.CacheCreateConfigOperation;
import com.hazelcast.client.ClientEndpoint;
import com.hazelcast.client.impl.client.ClientRequest;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;
import com.hazelcast.spi.InvocationBuilder;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.impl.SimpleExecutionCallback;

import java.io.IOException;
import java.security.Permission;

/**
 * This client request  specifically calls {@link CacheCreateConfigOperation} on the server side.
 *
 * @see com.hazelcast.cache.impl.operation.CacheCreateConfigOperation
 */
public class CacheCreateConfigRequest
        extends ClientRequest {

    private static final int TRY_COUNT = 100;

    private CacheConfig cacheConfig;
    private boolean createAlsoOnOthers = true;
    private boolean ignoreLocal;
    private int partitionId;

    public CacheCreateConfigRequest() {
    }

    public CacheCreateConfigRequest(CacheConfig cacheConfig, boolean createAlsoOnOthers, boolean ignoreLocal,
                                    int partitionId) {
        this.cacheConfig = cacheConfig;
        this.createAlsoOnOthers = createAlsoOnOthers;
        this.ignoreLocal = ignoreLocal;
        this.partitionId = partitionId;
    }

    @Override
    public final void process()
            throws Exception {
        final ClientEndpoint endpoint = getEndpoint();
        final Operation op = prepareOperation();
        op.setCallerUuid(endpoint.getUuid());
        final InvocationBuilder builder = operationService.createInvocationBuilder(getServiceName(), op, partitionId);
        builder.setTryCount(TRY_COUNT)
                .setResultDeserialized(false)
                .setExecutionCallback(new SimpleExecutionCallback<Object>() {
                    public void notify(Object object) {
                        endpoint.sendResponse(object, getCallId());
                    }
                });
        builder.invoke();
    }

    protected Operation prepareOperation() {
        return new CacheCreateConfigOperation(cacheConfig, createAlsoOnOthers, ignoreLocal);
    }

    @Override
    public final int getFactoryId() {
        return CachePortableHook.F_ID;
    }

    @Override
    public int getClassId() {
        return CachePortableHook.CREATE_CONFIG;
    }

    @Override
    public String getServiceName() {
        return CacheService.SERVICE_NAME;
    }

    @Override
    public void write(PortableWriter writer)
            throws IOException {
        writer.writeBoolean("o", createAlsoOnOthers);
        writer.writeBoolean("l", ignoreLocal);
        writer.writeInt("p", partitionId);
        final ObjectDataOutput out = writer.getRawDataOutput();
        out.writeObject(cacheConfig);
    }

    @Override
    public void read(PortableReader reader)
            throws IOException {
        createAlsoOnOthers = reader.readBoolean("o");
        ignoreLocal = reader.readBoolean("l");
        partitionId = reader.readInt("p");
        final ObjectDataInput in = reader.getRawDataInput();
        cacheConfig = in.readObject();
    }

    @Override
    public Permission getRequiredPermission() {
        return null;
    }

}
