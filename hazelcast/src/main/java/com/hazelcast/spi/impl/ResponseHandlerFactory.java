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
import com.hazelcast.spi.Callback;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.ResponseHandler;
import com.hazelcast.spi.exception.ResponseAlreadySentException;

import java.util.concurrent.atomic.AtomicBoolean;

public final class ResponseHandlerFactory {

    private static final NoResponseHandler NO_RESPONSE_HANDLER = new NoResponseHandler();

    public static void setRemoteResponseHandler(NodeEngine nodeEngine, Operation op) {
        op.setResponseHandler(createRemoteResponseHandler(nodeEngine, op));
    }

    public static ResponseHandler createRemoteResponseHandler(NodeEngine nodeEngine, Operation op) {
        if (op.getCallId() == 0) {
            if (op.returnsResponse()) {
                throw new HazelcastException("Op: " + op.getClass().getName() + " can not return response without call-id!");
            }
            return NO_RESPONSE_HANDLER;
        }
        return new RemoteInvocationResponseHandler(nodeEngine, op);
    }

    public static ResponseHandler createEmptyResponseHandler() {
        return NO_RESPONSE_HANDLER;
    }

    private static class NoResponseHandler implements ResponseHandler {
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

    private static class ErrorLoggingResponseHandler implements ResponseHandler {
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

    private static class RemoteInvocationResponseHandler implements ResponseHandler {

        private final NodeEngine nodeEngine;
        private final Operation op;
        private final AtomicBoolean sent = new AtomicBoolean(false);

        private RemoteInvocationResponseHandler(NodeEngine nodeEngine, Operation op) {
            this.nodeEngine = nodeEngine;
            this.op = op;
        }

        @Override
        public void sendResponse(Object obj) {
            long callId = op.getCallId();
            Connection conn = op.getConnection();
            if (!sent.compareAndSet(false, true) && !(obj instanceof Throwable)) {
                throw new ResponseAlreadySentException("NormalResponse already sent for call: " + callId
                        + " to " + conn.getEndPoint() + ", current-response: " + obj);
            }

            NormalResponse response;
            if (!(obj instanceof NormalResponse)) {
                response = new NormalResponse(obj, op.getCallId(), 0, op.isUrgent());
            } else {
                response = (NormalResponse) obj;
            }

            nodeEngine.getOperationService().send(response, op.getCallerAddress());
        }

        @Override
        public boolean isLocal() {
            return false;
        }
    }


    private ResponseHandlerFactory() {
    }
}
