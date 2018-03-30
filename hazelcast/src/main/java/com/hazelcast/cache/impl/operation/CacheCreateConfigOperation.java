/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.cache.impl.CacheDataSerializerHook;
import com.hazelcast.cache.impl.ICacheService;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.Member;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.OperationService;
import com.hazelcast.spi.impl.AbstractNamedOperation;
import com.hazelcast.spi.impl.SimpleExecutionCallback;

import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Used to create cluster wide cache configuration.
 * <p>
 * This configuration is created using the following algorithm;
 * <ul>
 * <li>Find partition ID using the distributed object name of cache as a key.</li>
 * <li>Send the <code>CacheCreateConfigOperation</code> operation to the calculated partition which will force all
 * clusters to be single threaded.</li>
 * <li>{@link ICacheService#putCacheConfigIfAbsent(com.hazelcast.config.CacheConfig)} is called.</li>
 * </ul>
 * <p>
 * This operation's purpose is to pass the required parameters into
 * {@link ICacheService#putCacheConfigIfAbsent(com.hazelcast.config.CacheConfig)}.
 * <p/>
 * Caveat: This operation does not return a response when {@code createAlsoOnOthers} is {@code true} and there are at least one
 * remote members on which the new {@code CacheConfig} needs to be created. When this operation returns {@code null} with {@code
 * createAlsoOnOthers == true}, it is impossible to tell whether it is because the {@code CacheConfig} was not already registered
 * or due to sending out the operation to other remote members.
 *
 * @deprecated as of 3.10 replaced by {@link AddCacheConfigOperation}, which is used in conjunction with
 * {@link com.hazelcast.internal.util.InvocationUtil#invokeOnStableClusterSerial(NodeEngine,
 * com.hazelcast.util.function.Supplier, int)} to reliably broadcast the {@code CacheConfig} to all members of the cluster.
 */
@Deprecated
public class CacheCreateConfigOperation
        extends AbstractNamedOperation
        implements IdentifiedDataSerializable {

    private CacheConfig config;
    private boolean createAlsoOnOthers = true;
    private boolean ignoreLocal;

    private boolean returnsResponse = true;
    private transient Object response;

    public CacheCreateConfigOperation() {
    }

    public CacheCreateConfigOperation(CacheConfig config, boolean createAlsoOnOthers) {
        this(config, createAlsoOnOthers, false);
    }

    public CacheCreateConfigOperation(CacheConfig config, boolean createAlsoOnOthers, boolean ignoreLocal) {
        super(config.getNameWithPrefix());
        this.config = config;
        this.createAlsoOnOthers = createAlsoOnOthers;
        this.ignoreLocal = ignoreLocal;
    }

    @Override
    public String getServiceName() {
        return ICacheService.SERVICE_NAME;
    }

    @Override
    public void run() throws Exception {
        ICacheService service = getService();
        if (!ignoreLocal) {
            response = service.putCacheConfigIfAbsent(config);
        }
        if (createAlsoOnOthers) {
            NodeEngine nodeEngine = getNodeEngine();
            Collection<Member> members = nodeEngine.getClusterService().getMembers();
            int remoteNodeCount = members.size() - 1;

            if (remoteNodeCount > 0) {
                postponeReturnResponse();

                ExecutionCallback<Object> callback = new CacheConfigCreateCallback(this, remoteNodeCount);
                OperationService operationService = nodeEngine.getOperationService();
                for (Member member : members) {
                    if (!member.localMember()) {
                        CacheCreateConfigOperation op = new CacheCreateConfigOperation(config, false);
                        operationService
                                .createInvocationBuilder(ICacheService.SERVICE_NAME, op, member.getAddress())
                                .setExecutionCallback(callback)
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

    @Override
    public void onExecutionFailure(Throwable e) {
        // Execution failed so we should enable `returnsResponse` flag to prevent waiting anymore
        returnsResponse = true;
        super.onExecutionFailure(e);
    }

    private static class CacheConfigCreateCallback extends SimpleExecutionCallback<Object> {

        final AtomicInteger counter;
        final CacheCreateConfigOperation operation;

        public CacheConfigCreateCallback(CacheCreateConfigOperation op, int count) {
            this.operation = op;
            this.counter = new AtomicInteger(count);
        }

        @Override
        public void notify(Object object) {
            if (counter.decrementAndGet() == 0) {
                operation.sendResponse(null);
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
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeObject(config);
        out.writeBoolean(createAlsoOnOthers);
        out.writeBoolean(ignoreLocal);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        config = in.readObject();
        createAlsoOnOthers = in.readBoolean();
        ignoreLocal = in.readBoolean();
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
