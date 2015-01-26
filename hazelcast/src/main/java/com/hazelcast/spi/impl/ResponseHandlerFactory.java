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
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationAccessor;
import com.hazelcast.spi.OperationService;
import com.hazelcast.spi.ResponseHandler;
import com.hazelcast.spi.exception.ResponseAlreadySentException;

public final class ResponseHandlerFactory {

    private static final NoResponseHandler NO_RESPONSE_HANDLER = new NoResponseHandler();
    private static final RemoteInvocationResponseHandler REMOTE_RESPONSE_HANDLER = new RemoteInvocationResponseHandler();

    private ResponseHandlerFactory() {
    }

    public static ResponseHandler createRemoteResponseHandler() {
        return REMOTE_RESPONSE_HANDLER;
    }

    public static ResponseHandler createEmptyResponseHandler() {
        return NO_RESPONSE_HANDLER;
    }

    private static class NoResponseHandler
            implements ResponseHandler {

        @Override
        public void sendResponse(Operation op, Object obj) {
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
        public void sendResponse(Operation op, Object obj) {
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

    public static final class RemoteInvocationResponseHandler implements ResponseHandler {

        @Override
        public void sendResponse(Operation operation, Object obj) {
            long callId = operation.getCallId();
            Connection conn = operation.getConnection();
            if (!OperationAccessor.setResponseSend(operation) && !(obj instanceof Throwable)) {
                throw new ResponseAlreadySentException("NormalResponse already sent for call: " + callId
                        + " to " + conn.getEndPoint() + ", current-response: " + obj);
            }

            Response response;
            if (!(obj instanceof Response)) {
                response = new NormalResponse(obj, callId, 0, operation.isUrgent());
            } else {
                response = (Response) obj;
            }

            NodeEngine nodeEngine = operation.getNodeEngine();
            InternalOperationService operationService = (InternalOperationService) nodeEngine.getOperationService();
            if (!operationService.send(response, operation.getCallerAddress())) {
                throw new HazelcastException("Cannot send response: " + obj + " to " + conn.getEndPoint());
            }
        }

        @Override
        public boolean isLocal() {
            return false;
        }
    }
}
