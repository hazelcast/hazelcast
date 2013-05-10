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

package com.hazelcast.client;

import com.hazelcast.nio.Address;
import com.hazelcast.spi.*;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * @mdogan 5/3/13
 */
public abstract class MultiTargetClientRequest extends ClientRequest {

    final void process() throws Exception {
        final ClientEndpoint endpoint = getEndpoint();
        OperationFactory operationFactory = createOperationFactory();
        Collection<Address> targets = getTargets();
        MultiTargetCallback callback = new MultiTargetCallback(targets);
        for (Address target : targets) {
            final Operation op = operationFactory.createOperation();
            op.setCallerUuid(endpoint.getUuid());
            final InvocationBuilder builder = clientEngine.createInvocationBuilder(getServiceName(), op, target)
                    .setTryCount(100)
                    .setCallback(new SingleTargetCallback(target, callback));
            Invocation inv = builder.build();
            inv.invoke();
        }
    }

    private class MultiTargetCallback {

        final Collection<Address> targets;
        final ConcurrentMap<Address, Object> results;

        private MultiTargetCallback(Collection<Address> targets) {
            this.targets = Collections.synchronizedSet(new HashSet<Address>(targets));
            results = new ConcurrentHashMap<Address, Object>(targets.size());
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
                final Object response = reduce(results);
                clientEngine.sendResponse(getEndpoint(), response);
            }
        }
    }

    private class SingleTargetCallback implements Callback<Object> {

        final Address target;
        final MultiTargetCallback parent;

        private SingleTargetCallback(Address target, MultiTargetCallback parent) {
            this.target = target;
            this.parent = parent;
        }

        public void notify(Object object) {
            parent.notify(target, object);
        }
    }

    protected abstract OperationFactory createOperationFactory();

    protected abstract Object reduce(Map<Address, Object> map);

    public abstract Collection<Address> getTargets();

}
