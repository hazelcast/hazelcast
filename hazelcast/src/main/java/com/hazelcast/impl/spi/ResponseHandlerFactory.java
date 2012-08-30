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

package com.hazelcast.impl.spi;

import com.hazelcast.nio.Connection;

import java.util.logging.Level;

/**
 * @mdogan 8/2/12
 */
public final class ResponseHandlerFactory {

    public static void setNoReplyResponseHandler(NodeService NodeService, Operation op) {
        op.setResponseHandler(new NoReplyResponseHandler(NodeService, op));
    }

    public static void setLocalResponseHandler(NodeService NodeService, SingleInvocation inv) {
        inv.getOperation().setResponseHandler(createLocalResponseHandler(NodeService, inv));
    }

    public static ResponseHandler createLocalResponseHandler(NodeService NodeService, SingleInvocation inv) {
        return new LocalInvocationResponseHandler(inv);
    }

    public static void setRemoteResponseHandler(NodeService NodeService, Operation op, final int partitionId, final long callId) {
        op.setResponseHandler(createRemoteResponseHandler(NodeService, op, partitionId, callId));
    }

    public static ResponseHandler createRemoteResponseHandler(NodeService NodeService, Operation op,
                                                              final int partitionId, final long callId) {
        return new RemoteInvocationResponseHandler(NodeService, op.getConnection(), partitionId, callId);
    }

    private static class NoReplyResponseHandler implements ResponseHandler {
        private final NodeService nodeService;
        private final Operation operation;

        private NoReplyResponseHandler(final NodeService NodeService, final Operation operation) {
            this.nodeService = NodeService;
            this.operation = operation;
        }

        public void sendResponse(final Object obj) {
            if (obj instanceof Throwable) {
                nodeService.getLogger(getClass().getName())
                        .log(Level.WARNING, "Error while executing operation: " + operation, (Throwable) obj);
            } else {
                throw new IllegalStateException("Can not send response for NoReply operation: "
                        + operation);
            }
        }
    }

    private static class RemoteInvocationResponseHandler implements ResponseHandler {

        private final NodeService nodeService;
        private final Connection conn;
        private final int partitionId;
        private final long callId;

        private RemoteInvocationResponseHandler(final NodeService nodeService, final Connection conn, final int partitionId,
                                                final long callId) {
            this.nodeService = nodeService;
            this.conn = conn;
            this.partitionId = partitionId;
            this.callId = callId;
        }

        public void sendResponse(Object response) {
            if (!(response instanceof Operation)) {
                response = new Response(response);
            }
            Operation responseOp = (Operation) response;
            responseOp.setCallId(callId);
            nodeService.send(responseOp, partitionId, conn);
        }
    }

    private static class LocalInvocationResponseHandler implements ResponseHandler {

        private final SingleInvocation invocation;

        private LocalInvocationResponseHandler(final SingleInvocation invocation) {
            this.invocation = invocation;
        }

        public void sendResponse(final Object obj) {
            invocation.setResult(obj);
        }
    }

    private ResponseHandlerFactory() {
    }
}
