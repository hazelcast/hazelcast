/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.spi.impl.operationservice.impl.OperationServiceImpl;

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

import static com.hazelcast.internal.util.MapUtil.createHashMap;
import static java.util.Collections.EMPTY_MAP;

public abstract class AbstractMultiTargetMessageTask<P> extends AbstractMessageTask<P> {

    protected AbstractMultiTargetMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected void processMessage() throws Throwable {
        Supplier<Operation> operationSupplier = createOperationSupplier();
        Collection<Member> targets = getTargets();

        returnResponseIfNoTargetLeft(targets, EMPTY_MAP);

        final OperationServiceImpl operationService = nodeEngine.getOperationService();

        MultiTargetCallback callback = new MultiTargetCallback(targets);
        for (Member target : targets) {
            Operation op = operationSupplier.get();
            InvocationBuilder builder = operationService.createInvocationBuilder(getServiceName(), op, target.getAddress())
                    .setResultDeserialized(false);
            builder.invoke().whenCompleteAsync(new SingleTargetCallback(target, callback));
        }
    }

    private void returnResponseIfNoTargetLeft(Collection<Member> targets, Map<Member, Object> results) throws Throwable {
        if (targets.isEmpty()) {
            sendResponse(reduce(results));
        }
    }

    protected abstract Supplier<Operation> createOperationSupplier();

    protected abstract Object reduce(Map<Member, Object> map) throws Throwable;

    public abstract Collection<Member> getTargets();

    private final class MultiTargetCallback {

        final Collection<Member> targets;
        final Map<Member, Object> results;

        private MultiTargetCallback(Collection<Member> targets) {
            this.targets = new HashSet<Member>(targets);
            this.results = createHashMap(targets.size());
        }

        public synchronized void notify(Member target, Object result) {
            if (targets.remove(target)) {
                results.put(target, result);
            } else {
                if (results.containsKey(target)) {
                    throw new IllegalArgumentException("Duplicate response from -> " + target);
                }
                throw new IllegalArgumentException("Unknown target! -> " + target);
            }
            try {
                returnResponseIfNoTargetLeft(targets, results);
            } catch (Throwable throwable) {
                handleProcessingFailure(throwable);
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
