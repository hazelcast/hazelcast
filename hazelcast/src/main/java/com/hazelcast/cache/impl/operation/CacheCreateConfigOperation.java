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

package com.hazelcast.cache.impl.operation;

import com.hazelcast.cache.impl.AbstractCacheService;
import com.hazelcast.cache.impl.CacheDataSerializerHook;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.Callback;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.OperationService;
import com.hazelcast.spi.ResponseHandler;
import com.hazelcast.spi.impl.AbstractNamedOperation;

import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Used to create cluster wide cache configuration.
 * <p>This configuration is created using the following algorithm;
 * <ul>
 * <li>Find partition id using the distributed object name of cache as a key.</li>
 * <li>Send the <code>CacheCreateConfigOperation</code> operation to the calculated partition which will force all
 * clusters to be single threaded.</li>
 * <li>{@link com.hazelcast.cache.impl.CacheService#createCacheConfigIfAbsent(com.hazelcast.config.CacheConfig)} is called.</li>
 * </ul></p>
 * <p>This operation's purpose is to pass the required parameters into
 * {@link com.hazelcast.cache.impl.CacheService#createCacheConfigIfAbsent(com.hazelcast.config.CacheConfig)}.</p>
 */
public class CacheCreateConfigOperation
        extends AbstractNamedOperation
        implements IdentifiedDataSerializable {

    private CacheConfig config;
    private boolean createAlsoOnOthers = true;

    private boolean returnsResponse = true;
    private transient Object response;

    public CacheCreateConfigOperation() {
    }

    public CacheCreateConfigOperation(CacheConfig config) {
        this(config, true);
    }

    public CacheCreateConfigOperation(CacheConfig config, boolean createAlsoOnOthers) {
        super(config.getNameWithPrefix());
        this.config = config;
        this.createAlsoOnOthers = createAlsoOnOthers;
    }

    @Override
    public void run() throws Exception {
        AbstractCacheService service = getService();
        response = service.createCacheConfigIfAbsent(config);

        if (createAlsoOnOthers && response == null) {
            NodeEngine nodeEngine = getNodeEngine();
            Collection<MemberImpl> members = nodeEngine.getClusterService().getMemberList();
            int remoteNodeCount = members.size() - 1;

            if (remoteNodeCount > 0) {
                postponeReturnResponse();

                Callback<Object> callback = new CacheConfigCreateCallback(getResponseHandler(), remoteNodeCount);
                OperationService operationService = nodeEngine.getOperationService();
                for (MemberImpl member : members) {
                    if (!member.localMember()) {
                        CacheCreateConfigOperation op = new CacheCreateConfigOperation(config, true);
                        operationService
                                .createInvocationBuilder(AbstractCacheService.SERVICE_NAME, op, member.getAddress())
                                .setCallback(callback)
                                .invoke();
                    }
                }
            }
        }
    }

    private void postponeReturnResponse() {
        // If config already exists or it's local-only created then return response immediately.
        // Otherwise response will be sent after config is created on all members.
        returnsResponse = false;
    }

    private static class CacheConfigCreateCallback implements Callback<Object> {

        final ResponseHandler responseHandler;
        final AtomicInteger counter;

        public CacheConfigCreateCallback(ResponseHandler responseHandler, int count) {
            this.responseHandler = responseHandler;
            counter = new AtomicInteger(count);
        }

        @Override
        public void notify(Object object) {
            if (counter.decrementAndGet() == 0) {
                responseHandler.sendResponse(null);
            }
        }
    }

    @Override
    public Object getResponse() {
        return response;
    }

    @Override
    public boolean returnsResponse() {
        return returnsResponse;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out)
            throws IOException {
        super.writeInternal(out);
        out.writeObject(config);
        out.writeBoolean(createAlsoOnOthers);
    }

    @Override
    protected void readInternal(ObjectDataInput in)
            throws IOException {
        super.readInternal(in);
        config = in.readObject();
        createAlsoOnOthers = in.readBoolean();
    }

    @Override
    public int getId() {
        return CacheDataSerializerHook.CREATE_CONFIG;
    }

    @Override
    public int getFactoryId() {
        return CacheDataSerializerHook.F_ID;
    }

}
