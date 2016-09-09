/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.impl.job;


import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.jet.impl.operation.JetOperation;
import com.hazelcast.nio.Address;
import com.hazelcast.spi.InvocationBuilder;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.OperationService;

import java.util.concurrent.CompletableFuture;

public class ServerJobInvocation<T> extends AbstractJobInvocation<JetOperation, T> {
    private final NodeEngine nodeEngine;

    public ServerJobInvocation(JetOperation operation, Address address, NodeEngine nodeEngine) {
        super(operation, address);
        this.nodeEngine = nodeEngine;
    }

    @Override
    @SuppressWarnings("unchecked")
    protected CompletableFuture<T> getFuture() {
        final CompletableFuture<T> completableFuture = new CompletableFuture<>();
        OperationService os = nodeEngine.getOperationService();
        InvocationBuilder ib = os.createInvocationBuilder(JobService.SERVICE_NAME, operation, address);
        ib.invoke().andThen(new ExecutionCallback<Object>() {
            @Override
            public void onResponse(Object o) {
                completableFuture.complete((T) o);
            }

            @Override
            public void onFailure(Throwable throwable) {
                completableFuture.completeExceptionally(throwable);
            }
        });
        return completableFuture;
    }
}
