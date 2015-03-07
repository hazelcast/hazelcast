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

package com.hazelcast.client.impl.messagehandlers;

import com.hazelcast.client.ClientEndpoint;
import com.hazelcast.client.impl.protocol.MessageHandlerContext;
import com.hazelcast.client.impl.protocol.MessageHandlerParameters;
import com.hazelcast.spi.Callback;
import com.hazelcast.spi.InvocationBuilder;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationService;

abstract class AbstractPartitionsMessageHandler extends AbstractMessageHandler {
    private static final int TRY_COUNT = 100;

    /**
     * Called on node side, before starting any operation.
     */
    protected void beforeProcess(MessageHandlerContext context) {
    }

    /**
     * Called on node side, after process is run and right before sending the response to the client.
     */
    protected void beforeResponse(MessageHandlerContext context) {
    }

    /**
     * Called on node side, after sending the response to the client.
     */
    protected void afterResponse(MessageHandlerContext context) {
    }

    @Override
    public final void process(MessageHandlerContext context) {
        final OperationService opService = context.getOperationService();
        beforeProcess(context);
        ClientEndpoint endpoint = context.getClientEndpoint();
        Operation op = prepareOperation(context);
        op.setCallerUuid(endpoint.getUuid());
        InvocationBuilder builder = opService.createInvocationBuilder(getServiceName(), op, getPartition(context))
                .setReplicaIndex(getReplicaIndex())
                .setTryCount(TRY_COUNT)
                .setResultDeserialized(false)
                .setCallback(new CallbackImpl(context));
        builder.invoke();
    }

    protected abstract String getServiceName();

    protected abstract Operation prepareOperation(MessageHandlerContext context);

    protected abstract int getPartition(MessageHandlerContext context);

    protected int getReplicaIndex() {
        return 0;
    }

    protected Object filter(Object response, MessageHandlerParameters parameters) {
        return response;
    }

    private static class CallbackImpl implements Callback<Object> {
        private final ClientEndpoint endpoint;
        private final MessageHandlerContext context;

        public CallbackImpl(MessageHandlerContext context) {
            this.context = context;
            this.endpoint = context.getClientEndpoint();
        }

        @Override
        public void notify(Object object) {

        }
    }
}
