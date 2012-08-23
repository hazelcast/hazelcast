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
import com.hazelcast.nio.SimpleSocketWritable;

import java.util.logging.Level;

import static com.hazelcast.impl.ClusterOperation.REMOTE_CALL;
import static com.hazelcast.nio.IOUtil.toData;

/**
 * @mdogan 8/2/12
 */
public final class ResponseHandlerFactory {

    public static void setNoReplyResponseHandler(NodeService nodeService, Operation op) {
        op.setResponseHandler(new NoReplyResponseHandler(nodeService, op));
    }

    public static void setLocalResponseHandler(NodeService nodeService, SingleInvocation inv) {
        inv.getOperation().setResponseHandler(createLocalResponseHandler(nodeService, inv));
    }

    public static ResponseHandler createLocalResponseHandler(NodeService nodeService, SingleInvocation inv) {
        if (inv.getOperation() instanceof NoReply) {
            return new NoReplyResponseHandler(nodeService, inv.getOperation());
        }
        return new LocalInvocationResponseHandler(inv);
    }

    public static void setRemoteResponseHandler(NodeService nodeService, Operation op,
                                         final int partitionId, final int replicaIndex, final long callId,
                                         final String serviceName) {
        op.setResponseHandler(
                createRemoteResponseHandler(nodeService, op, partitionId, replicaIndex, callId, serviceName));
    }

    public static ResponseHandler createRemoteResponseHandler(NodeService nodeService, Operation op,
                                                       final int partitionId, final int replicaIndex, final long callId,
                                                       final String serviceName) {
        if (op instanceof NoReply) {
            return new NoReplyResponseHandler(nodeService, op);
        }
        return new RemoteInvocationResponseHandler(nodeService, op.getConnection(), partitionId, replicaIndex, callId, serviceName);
    }

    private static class NoReplyResponseHandler implements ResponseHandler {
        private final NodeService nodeService;
        private final Operation operation;

        private NoReplyResponseHandler(final NodeService nodeService, final Operation operation) {
            this.nodeService = nodeService;
            this.operation = operation;
        }

        public void sendResponse(final Object obj) {
            if (obj instanceof Throwable) {
                nodeService.getNode().getLogger(getClass().getName())
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
        private final int replicaIndex;
        private final long callId;
        private final String serviceName;

        private RemoteInvocationResponseHandler(final NodeService nodeService, final Connection conn,
                                                final int partitionId, final int replicaIndex,
                                                final long callId, final String serviceName) {
            this.nodeService = nodeService;
            this.conn = conn;
            this.replicaIndex = replicaIndex;
            this.partitionId = partitionId;
            this.callId = callId;
            this.serviceName = serviceName;
        }

        public void sendResponse(Object response) {
            if (!(response instanceof Operation)) {
                response = new Response(response);
            }
            boolean nb = (response instanceof NonBlockingOperation);
            nodeService.getNode().clusterImpl.send(new SimpleSocketWritable(REMOTE_CALL, serviceName,
                    toData(response), callId, partitionId, replicaIndex, null, nb), conn);
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
