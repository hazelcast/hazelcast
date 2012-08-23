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

import com.hazelcast.nio.Packet;

import java.util.logging.Level;

import static com.hazelcast.nio.IOUtil.toData;

/**
 * @mdogan 8/2/12
 */
final class ResponseHandlerFactory {

    static ResponseHandler createLocalResponseHandler(NodeService nodeService, SingleInvocation inv) {
        if (inv.getOperation() instanceof NoReply) {
            return new NoReplyResponseHandler(nodeService, inv.getOperation());
        }
        return new LocalInvocationResponseHandler(inv);
    }

    static ResponseHandler createRemoteResponseHandler(NodeService nodeService, Operation op, Packet packet,
                                                       final int partitionId, final long callId, final String serviceName) {
        if (op instanceof NoReply) {
            return new NoReplyResponseHandler(nodeService, op);
        }
        return new RemoteInvocationResponseHandler(nodeService, packet, partitionId, callId, serviceName);
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
        private final Packet packet;
        private final int partitionId;
        private final long callId;
        private final String serviceName;

        private RemoteInvocationResponseHandler(final NodeService nodeService, final Packet packet,
                                                final int partitionId, final long callId, final String serviceName) {
            this.nodeService = nodeService;
            this.packet = packet;
            this.partitionId = partitionId;
            this.callId = callId;
            this.serviceName = serviceName;
        }

        public void sendResponse(Object response) {
            if (!(response instanceof Operation)) {
                response = new Response(response);
            }
            packet.clearForResponse();
            packet.blockId = partitionId;
            packet.callId = callId;
            packet.name = serviceName;
            packet.longValue = (response instanceof NonBlockingOperation) ? 1 : 0;
            packet.setValue(toData(response));
//            nodeService.sendPacket(packet);
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
