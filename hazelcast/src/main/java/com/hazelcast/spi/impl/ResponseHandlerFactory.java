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

package com.hazelcast.spi.impl;

import com.hazelcast.core.HazelcastException;
import com.hazelcast.spi.*;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @mdogan 8/2/12
 */
public final class ResponseHandlerFactory {

    private static final NoResponseHandler NO_RESPONSE_HANDLER = new NoResponseHandler();

    public static void setLocalResponseHandler(InvocationImpl inv) {
        inv.getOperation().setResponseHandler(createLocalResponseHandler(inv));
    }

    public static ResponseHandler createLocalResponseHandler(InvocationImpl inv) {
        return new LocalInvocationResponseHandler(inv);
    }

    public static void setRemoteResponseHandler(NodeEngine nodeEngine, Operation op) {
        op.setResponseHandler(createRemoteResponseHandler(nodeEngine, op));
    }

    public static ResponseHandler createRemoteResponseHandler(NodeEngine nodeEngine, Operation op) {
        if (op.getCallId() < 0) {
            if (op.returnsResponse()) {
                throw new HazelcastException("Op: " + op.getClass().getName() + " can not return response without call-id!");
            }
            return NO_RESPONSE_HANDLER;
        }
        return new RemoteInvocationResponseHandler(nodeEngine, op.getConnection(), op.getPartitionId(), op.getCallId());
    }

    public static ResponseHandler createEmptyResponseHandler() {
        return NO_RESPONSE_HANDLER;
    }

    private static class NoResponseHandler implements ResponseHandler {
        public void sendResponse(final Object obj) {
        }
    }

    private static class RemoteInvocationResponseHandler implements ResponseHandler {

        private final NodeEngine nodeEngine;
        private final Connection conn;
        private final int partitionId;
        private final long callId;
        private final AtomicBoolean sent = new AtomicBoolean(false);

        private RemoteInvocationResponseHandler(NodeEngine nodeEngine, Connection conn,
                                                int partitionId, long callId) {
            this.nodeEngine = nodeEngine;
            this.conn = conn;
            this.partitionId = partitionId;
            this.callId = callId;
        }

        public void sendResponse(Object obj) {
            if (!sent.compareAndSet(false, true)) {
                throw new IllegalStateException("Response already sent for call: " + callId
                                                + " to " + conn.getEndPoint());
            }
            final Operation response;
            if (obj instanceof Operation) {
                response = (Operation) obj;
            } else {
                response = new Response(obj);
            }
            OperationAccessor.setCallId(response, callId);
            response.setPartitionId(partitionId);
            nodeEngine.getOperationService().send(response, conn);
        }
    }

    private static class LocalInvocationResponseHandler implements ResponseHandler {

        private final InvocationImpl invocation;
        private final AtomicBoolean sent = new AtomicBoolean(false);

        private LocalInvocationResponseHandler(InvocationImpl invocation) {
            this.invocation = invocation;
        }

        public void sendResponse(Object obj) {
            if (!sent.compareAndSet(false, true)) {
                throw new IllegalStateException("Response already sent for invocation: " + invocation);
            }
            invocation.notify(obj);
        }
    }

    private ResponseHandlerFactory() {
    }
}
