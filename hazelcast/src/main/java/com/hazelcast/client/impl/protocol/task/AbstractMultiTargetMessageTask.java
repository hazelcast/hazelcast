/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.impl.protocol.task;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.cluster.Member;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.nio.Connection;
import com.hazelcast.spi.impl.operationservice.InvocationBuilder;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.operationservice.impl.InvocationFuture;
import com.hazelcast.spi.impl.operationservice.impl.OperationServiceImpl;

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

import static com.hazelcast.internal.util.ConcurrencyUtil.CALLER_RUNS;
import static com.hazelcast.internal.util.MapUtil.createHashMap;
import static java.util.Collections.EMPTY_MAP;

/**
 * Base class for messages which are broadcast to a set of members
 * determined by {@link #getTargets()}.
 */
public abstract class AbstractMultiTargetMessageTask<P> extends AbstractAsyncMessageTask<P, Object> {

    protected AbstractMultiTargetMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected CompletableFuture<Object> processInternal() {
        Supplier<Operation> operationSupplier = createOperationSupplier();
        Collection<Member> targets = getTargets();

        CompletableFuture<Object> finalResult = new CompletableFuture<>();

        if (targets.isEmpty()) {
            finalResult.complete(EMPTY_MAP);
            return finalResult;
        }

        final OperationServiceImpl operationService = nodeEngine.getOperationService();

        MultiTargetCallback callback = new MultiTargetCallback(targets, finalResult);
        for (Member target : targets) {
            Operation op = operationSupplier.get();
            op.setCallerUuid(endpoint.getUuid());
            InvocationBuilder builder = operationService.createInvocationBuilder(getServiceName(), op, target.getAddress())
                                                        .setResultDeserialized(false);

            InvocationFuture<Object> invocationFuture = builder.invoke();
            invocationFuture.whenCompleteAsync(new SingleTargetCallback(target, callback), CALLER_RUNS);
        }

        return finalResult;
    }

    protected abstract Supplier<Operation> createOperationSupplier();

    /**
     * Reduce the responses from all targets into a single object.
     */
    protected abstract Object reduce(Map<Member, Object> map) throws Throwable;

    /**
     * Return the collection of members to which to send the operation.
     */
    public abstract Collection<Member> getTargets();

    private final class MultiTargetCallback {

        final Collection<Member> targets;
        final Map<Member, Object> results;
        final CompletableFuture<Object> finalResult;

        private MultiTargetCallback(Collection<Member> targets, CompletableFuture<Object> finalResult) {
            this.targets = new HashSet<>(targets);
            this.results = createHashMap(targets.size());
            this.finalResult = finalResult;
        }

        public synchronized void notify(Member target, Object result) {
            if (targets.remove(target)) {
                results.put(target, result);
            } else {
                if (results.containsKey(target)) {
                    finalResult.completeExceptionally(new IllegalArgumentException("Duplicate response from -> " + target));
                }
                finalResult.completeExceptionally(new IllegalArgumentException("Unknown target! -> " + target));
            }

            if (targets.isEmpty()) {
                try {
                    finalResult.complete(reduce(results));
                } catch (Throwable throwable) {
                    finalResult.completeExceptionally(throwable);
                }
            }
        }
    }

    private final class SingleTargetCallback implements BiConsumer<Object, Throwable> {

        final Member target;
        final MultiTargetCallback parent;

        private SingleTargetCallback(Member target, MultiTargetCallback parent) {
            this.target = target;
            this.parent = parent;
        }

        @Override
        public void accept(Object object, Throwable throwable) {
            parent.notify(target, throwable == null ? object : throwable);
        }
    }
}
