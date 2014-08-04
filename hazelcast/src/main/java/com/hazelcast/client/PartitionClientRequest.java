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

import com.hazelcast.spi.Callback;
import com.hazelcast.spi.InvocationBuilder;
import com.hazelcast.spi.Operation;

public abstract class PartitionClientRequest extends ClientRequest {

    private static final int TRY_COUNT = 100;

    protected void beforeProcess() {
    }

    protected void beforeResponse() {
    }

    protected void afterResponse(){
    }

    @Override
    final void process() {
        beforeProcess();
        ClientEndpoint endpoint = getEndpoint();
        Operation op = prepareOperation();
        op.setCallerUuid(endpoint.getUuid());
        InvocationBuilder builder = operationService.createInvocationBuilder(getServiceName(), op, getPartition())
                .setReplicaIndex(getReplicaIndex())
                .setTryCount(TRY_COUNT)
                .setResultDeserialized(false)
                .setCallback(new CallbackImpl(endpoint));
        builder.invoke();
    }

    protected abstract Operation prepareOperation();

    protected abstract int getPartition();

    protected int getReplicaIndex() {
        return 0;
    }

    protected Object filter(Object response) {
        return response;
    }

    private class CallbackImpl implements Callback<Object> {
        private final ClientEndpoint endpoint;

        public CallbackImpl(ClientEndpoint endpoint) {
            this.endpoint = endpoint;
        }

        @Override
        public void notify(Object object) {
            beforeResponse();
            endpoint.sendResponse(filter(object), getCallId());
        }
    }
}
