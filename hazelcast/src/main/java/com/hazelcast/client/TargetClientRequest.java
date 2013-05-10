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
import com.hazelcast.spi.Callback;
import com.hazelcast.spi.Invocation;
import com.hazelcast.spi.InvocationBuilder;
import com.hazelcast.spi.Operation;

/**
 * @mdogan 5/3/13
 */
public abstract class TargetClientRequest extends ClientRequest {

    final void process() throws Exception {
        final ClientEndpoint endpoint = getEndpoint();
        final Operation op = prepareOperation();
        op.setCallerUuid(endpoint.getUuid());
        final InvocationBuilder builder = clientEngine.createInvocationBuilder(getServiceName(), op, getTarget())
                .setTryCount(100)
                .setCallback(new Callback<Object>() {
                    public void notify(Object object) {
                        clientEngine.sendResponse(endpoint, filter(object));
                    }
                });
        Invocation inv = builder.build();
        inv.invoke();
    }

    protected abstract Operation prepareOperation();

    public abstract Address getTarget();

    protected Object filter(Object response) {
        return response;
    }

}
