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
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Connection;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.OperationService;
import com.hazelcast.spi.ResponseHandler;
import com.hazelcast.spi.exception.ResponseAlreadySentException;

import java.util.concurrent.atomic.AtomicBoolean;

public final class ResponseHandlerFactory {

    private static final NoResponseHandler NO_RESPONSE_HANDLER = new NoResponseHandler();

    private ResponseHandlerFactory() {
    }

    public static void setRemoteResponseHandler(NodeEngine nodeEngine, RemotePropagatable remotePropagatable) {
        remotePropagatable.setResponseHandler(createRemoteResponseHandler(nodeEngine, remotePropagatable));
    }

    public static ResponseHandler createRemoteResponseHandler(NodeEngine nodeEngine, RemotePropagatable remotePropagatable) {
        if (remotePropagatable.getCallId() == 0) {
            if (remotePropagatable.returnsResponse()) {
                throw new HazelcastException(
                        "Op: " + remotePropagatable.getClass().getName() + " can not return response without call-id!");
            }
            return NO_RESPONSE_HANDLER;
        }
        return new RemoteInvocationResponseHandler(nodeEngine, remotePropagatable);
    }

    public static ResponseHandler createEmptyResponseHandler() {
        return NO_RESPONSE_HANDLER;
    }

    private static class NoResponseHandler
            implements ResponseHandler {

        @Override
        public void sendResponse(final Object obj) {
        }

        @Override
        public boolean isLocal() {
            return false;
        }
    }

    public static ResponseHandler createErrorLoggingResponseHandler(ILogger logger) {
        return new ErrorLoggingResponseHandler(logger);
    }

    private static final class ErrorLoggingResponseHandler implements ResponseHandler {
        private final ILogger logger;

        private ErrorLoggingResponseHandler(ILogger logger) {
            this.logger = logger;
        }

        @Override
        public void sendResponse(final Object obj) {
            if (obj instanceof Throwable) {
                Throwable t = (Throwable) obj;
                logger.severe(t);
            }
        }

        @Override
        public boolean isLocal() {
            return true;
        }
    }

    private static final class RemoteInvocationResponseHandler implements ResponseHandler {

        private final NodeEngine nodeEngine;
        private final RemotePropagatable remotePropagatable;
        private final AtomicBoolean sent = new AtomicBoolean(false);

        private RemoteInvocationResponseHandler(NodeEngine nodeEngine, RemotePropagatable remotePropagatable) {
            this.nodeEngine = nodeEngine;
            this.remotePropagatable = remotePropagatable;
        }

        @Override
        public void sendResponse(Object obj) {
            long callId = remotePropagatable.getCallId();
            Connection conn = remotePropagatable.getConnection();
            if (!sent.compareAndSet(false, true) && !(obj instanceof Throwable)) {
                throw new ResponseAlreadySentException("NormalResponse already sent for call: " + callId
                        + " to " + conn.getEndPoint() + ", current-response: " + obj);
            }

            Response response;
            if (!(obj instanceof Response)) {
                response = new NormalResponse(obj, remotePropagatable.getCallId(), 0, remotePropagatable.isUrgent());
            } else {
                response = (Response) obj;
            }

            OperationService operationService = nodeEngine.getOperationService();
            if (!operationService.send(response, remotePropagatable.getCallerAddress())) {
                throw new HazelcastException("Cannot send response: " + obj + " to " + conn.getEndPoint());
            }
        }

        @Override
        public boolean isLocal() {
            return false;
        }
    }
}
