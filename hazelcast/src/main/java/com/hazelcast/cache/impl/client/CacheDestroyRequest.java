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
import com.hazelcast.cache.impl.operation.CacheDestroyOperation;
import com.hazelcast.client.ClientEndpoint;
import com.hazelcast.client.impl.client.ClientRequest;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;
import com.hazelcast.spi.Callback;
import com.hazelcast.spi.InvocationBuilder;
import com.hazelcast.spi.Operation;

import java.io.IOException;
import java.security.Permission;

/**
 * This client request  specifically calls {@link CacheDestroyOperation} on server side.
 * @see com.hazelcast.cache.impl.operation.CacheDestroyOperation
 */
public class CacheDestroyRequest
        extends ClientRequest {

    private static final int TRY_COUNT = 100;

    private String name;
    private int partitionId;

    public CacheDestroyRequest() {
    }

    public CacheDestroyRequest(String name, int partitionId) {
        this.name = name;
        this.partitionId = partitionId;
    }

    @Override
    public final void process()
            throws Exception {
        final ClientEndpoint endpoint = getEndpoint();
        final Operation op = prepareOperation();
        op.setCallerUuid(endpoint.getUuid());
        final InvocationBuilder builder = operationService.createInvocationBuilder(getServiceName(), op, partitionId);
        builder.setTryCount(TRY_COUNT).setResultDeserialized(false).setCallback(new Callback<Object>() {
            public void notify(Object object) {
                endpoint.sendResponse(object, getCallId());
            }
        });
        builder.invoke();
    }

    protected Operation prepareOperation() {
        return new CacheDestroyOperation(name);
    }

    public final int getFactoryId() {
        return CachePortableHook.F_ID;
    }

    public int getClassId() {
        return CachePortableHook.DESTROY_CACHE;
    }

    @Override
    public String getServiceName() {
        return CacheService.SERVICE_NAME;
    }

    public void write(PortableWriter writer)
            throws IOException {
        writer.writeUTF("n", name);
        writer.writeInt("p", partitionId);
    }

    public void read(PortableReader reader)
            throws IOException {
        name = reader.readUTF("n");
        partitionId = reader.readInt("p");
    }

    @Override
    public Permission getRequiredPermission() {
        return null;
    }
}
