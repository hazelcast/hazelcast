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

public abstract class TargetClientRequest extends ClientRequest {

    private static final int TRY_COUNT = 100;

    @Override
    public final void process() throws Exception {
        final ClientEndpoint endpoint = getEndpoint();
        Operation op = prepareOperation();
        op.setCallerUuid(endpoint.getUuid());
        InvocationBuilder builder = operationService.createInvocationBuilder(getServiceName(), op, getTarget())
                .setTryCount(TRY_COUNT)
                .setResultDeserialized(false)
                .setCallback(new Callback<Object>() {
                    public void notify(Object object) {
                        endpoint.sendResponse(filter(object), getCallId());
                    }
                });
        builder.invoke();
    }

    protected abstract Operation prepareOperation();

    public abstract Address getTarget();

    protected Object filter(Object response) {
        return response;
    }
}
