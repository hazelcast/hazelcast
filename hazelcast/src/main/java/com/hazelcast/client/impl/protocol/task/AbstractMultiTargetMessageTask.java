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

package com.hazelcast.client.impl.protocol.task;

import com.hazelcast.client.ClientEndpoint;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.instance.Node;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.Connection;
import com.hazelcast.spi.InvocationBuilder;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationFactory;
import com.hazelcast.spi.impl.SimpleExecutionCallback;
import com.hazelcast.spi.impl.operationservice.InternalOperationService;

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static java.util.Collections.EMPTY_MAP;
import static java.util.Collections.synchronizedSet;

public abstract class AbstractMultiTargetMessageTask<P> extends AbstractMessageTask<P> {

    private static final int TRY_COUNT = 100;

    protected AbstractMultiTargetMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected void processMessage() {
        ClientEndpoint endpoint = getEndpoint();
        OperationFactory operationFactory = createOperationFactory();
        Collection<Address> targets = getTargets();

        if (returnResponseIfNoTargetLeft(targets, EMPTY_MAP)) {
            return;
        }

        final InternalOperationService operationService = nodeEngine.getOperationService();

        MultiTargetCallback callback = new MultiTargetCallback(targets);
        for (Address target : targets) {
            Operation op = operationFactory.createOperation();
            op.setCallerUuid(endpoint.getUuid());
            InvocationBuilder builder = operationService.createInvocationBuilder(getServiceName(), op, target)
                    .setTryCount(TRY_COUNT)
                    .setResultDeserialized(false)
                    .setExecutionCallback(new SingleTargetCallback(target, callback));
            builder.invoke();
        }
    }

    private boolean returnResponseIfNoTargetLeft(Collection<Address> targets, Map<Address, Object> results) {
        if (targets.isEmpty()) {
            try {
                sendResponse(reduce(results));
            } catch (Throwable throwable) {
                sendClientMessage(throwable);
            }
            return true;
        }
        return false;
    }

    protected abstract OperationFactory createOperationFactory();

    protected abstract Object reduce(Map<Address, Object> map) throws Throwable;

    public abstract Collection<Address> getTargets();

    private final class MultiTargetCallback {

        final Collection<Address> targets;
        final Map<Address, Object> results;

        private MultiTargetCallback(Collection<Address> targets) {
            this.targets = synchronizedSet(new HashSet<Address>(targets));
            this.results = new ConcurrentHashMap<Address, Object>(targets.size());
        }

        public void notify(Address target, Object result) {
            if (targets.remove(target)) {
                results.put(target, result);
            } else {
                if (results.containsKey(target)) {
                    throw new IllegalArgumentException("Duplicate response from -> " + target);
                }
                throw new IllegalArgumentException("Unknown target! -> " + target);
            }
            returnResponseIfNoTargetLeft(targets, results);
        }
    }

    private final class SingleTargetCallback extends SimpleExecutionCallback<Object> {

        final Address target;
        final MultiTargetCallback parent;

        private SingleTargetCallback(Address target, MultiTargetCallback parent) {
            this.target = target;
            this.parent = parent;
        }

        @Override
        public void notify(Object object) {
            parent.notify(target, object);
        }
    }
}
