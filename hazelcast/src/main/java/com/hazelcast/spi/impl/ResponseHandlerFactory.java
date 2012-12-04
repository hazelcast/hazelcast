/*
 * Copyright (c) 2008-2012, Hazel Bilisim Ltd. All Rights Reserved.
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

import com.hazelcast.nio.Connection;
import com.hazelcast.spi.NodeService;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.ResponseHandler;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @mdogan 8/2/12
 */
public final class ResponseHandlerFactory {

//    public static void setNoReplyResponseHandler(NodeService nodeservice, Operation op) {
//        op.setResponseHandler(new NoReplyResponseHandler(nodeservice, op));
//    }

    public static void setLocalResponseHandler(InvocationImpl inv) {
        inv.getOperation().setResponseHandler(createLocalResponseHandler(inv));
    }

    public static ResponseHandler createLocalResponseHandler(InvocationImpl inv) {
        return new LocalInvocationResponseHandler(inv);
    }

    public static void setRemoteResponseHandler(NodeService nodeservice, Operation op) {
        op.setResponseHandler(createRemoteResponseHandler(nodeservice, op));
    }

    public static ResponseHandler createRemoteResponseHandler(NodeService nodeService, Operation op) {
        return new RemoteInvocationResponseHandler(nodeService, op.getConnection(), op.getPartitionId(), op.getCallId());
    }

//    private static class NoReplyResponseHandler implements ResponseHandler {
//        private final NodeService nodeService;
//        private final Operation operation;
//
//        private NoReplyResponseHandler(NodeService nodeService, Operation operation) {
//            this.nodeService = nodeService;
//            this.operation = operation;
//        }
//
//        public void sendResponse(final Object obj) {
//            if (obj instanceof Throwable) {
//                nodeService.getLogger(getClass().getName())
//                        .log(Level.WARNING, "Error while executing operation: " + operation, (Throwable) obj);
//            } else {
//                throw new IllegalStateException("Can not send response for NoReply operation: "
//                        + operation);
//            }
//        }
//    }

    private static class RemoteInvocationResponseHandler implements ResponseHandler {

        private final NodeService nodeService;
        private final Connection conn;
        private final int partitionId;
        private final long callId;
        private final AtomicBoolean sent = new AtomicBoolean(false);

        private RemoteInvocationResponseHandler(NodeService nodeService, Connection conn,
                                                int partitionId, long callId) {
            this.nodeService = nodeService;
            this.conn = conn;
            this.partitionId = partitionId;
            this.callId = callId;
        }

        public void sendResponse(Object response) {
            if (!sent.compareAndSet(false, true)) {
                throw new IllegalStateException("Response already sent for call: " + callId
                                                + " to " + conn.getEndPoint());
            }
            if (!(response instanceof Operation)) {
                response = new Response(response);
            }
            Operation responseOp = (Operation) response;
            responseOp.setCallId(callId);
            nodeService.send(responseOp, partitionId, conn);
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

    static class ResponseHandlerDelegate implements ResponseHandler {

        private final Operation operation;
        private Object response;

        ResponseHandlerDelegate(final Operation operation) {
            this.operation = operation;
        }

        public void sendResponse(final Object obj) {
            response = obj;
            final ResponseHandler responseHandler = operation.getResponseHandler();
            if (responseHandler != null) {
                responseHandler.sendResponse(obj);
            }
        }

        public Object getResponse() {
            return response;
        }
    }

    private ResponseHandlerFactory() {
    }
}
