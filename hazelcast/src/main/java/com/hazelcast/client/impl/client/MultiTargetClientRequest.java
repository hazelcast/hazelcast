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

package com.hazelcast.client.impl.client;

import com.hazelcast.client.ClientEndpoint;
import com.hazelcast.nio.Address;
import com.hazelcast.spi.Callback;
import com.hazelcast.spi.InvocationBuilder;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationFactory;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static java.util.Collections.synchronizedSet;

/**
 * Base class for client request that will be send more than one member
 */
public abstract class MultiTargetClientRequest extends ClientRequest {

    private static final int TRY_COUNT = 100;

    @Override
    public final void process() throws Exception {
        ClientEndpoint endpoint = getEndpoint();
        OperationFactory operationFactory = createOperationFactory();
        Collection<Address> targets = getTargets();
        if (targets.isEmpty()) {
            endpoint.sendResponse(reduce(new HashMap<Address, Object>()), getCallId());
            return;
        }

        MultiTargetCallback callback = new MultiTargetCallback(targets);
        for (Address target : targets) {
            Operation op = operationFactory.createOperation();
            op.setCallerUuid(endpoint.getUuid());
            InvocationBuilder builder = operationService.createInvocationBuilder(getServiceName(), op, target)
                    .setTryCount(TRY_COUNT)
                    .setResultDeserialized(false)
                    .setCallback(new SingleTargetCallback(target, callback));
            builder.invoke();
        }
    }


    protected abstract OperationFactory createOperationFactory();

    protected abstract Object reduce(Map<Address, Object> map);

    public abstract Collection<Address> getTargets();

    private final class MultiTargetCallback {

        final Collection<Address> targets;
        final ConcurrentMap<Address, Object> results;

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
            if (targets.isEmpty()) {
                Object response = reduce(results);
                endpoint.sendResponse(response, getCallId());
            }
        }
    }

    private static final class SingleTargetCallback implements Callback<Object> {

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
