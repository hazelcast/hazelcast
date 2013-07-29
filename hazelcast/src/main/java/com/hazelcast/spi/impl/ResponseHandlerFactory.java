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
import com.hazelcast.spi.*;
import com.hazelcast.spi.exception.ResponseAlreadySentException;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;

/**
 * @author mdogan 8/2/12
 */
public final class ResponseHandlerFactory {

    private static final NoResponseHandler NO_RESPONSE_HANDLER = new NoResponseHandler();

    public static void setLocalResponseHandler(Operation op, Callback<Object> callback) {
        op.setResponseHandler(createLocalResponseHandler(op, callback));
    }

    public static ResponseHandler createLocalResponseHandler(Operation op, Callback<Object> callback) {
        return new LocalInvocationResponseHandler(callback, op.getCallId());
    }

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
        return new RemoteInvocationResponseHandler(nodeEngine, op.getConnection(), op.getCallId());
    }

    public static ResponseHandler createEmptyResponseHandler() {
        return NO_RESPONSE_HANDLER;
    }

    private static class NoResponseHandler implements ResponseHandler {
        public void sendResponse(final Object obj) {
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

        public void sendResponse(final Object obj) {
            if (obj instanceof Throwable) {
                Throwable t = (Throwable) obj;
                logger.severe(t);
            }
        }
    }

    private static class RemoteInvocationResponseHandler implements ResponseHandler {

        private final NodeEngine nodeEngine;
        private final Connection conn;
        private final long callId;
        private final AtomicBoolean sent = new AtomicBoolean(false);

        private RemoteInvocationResponseHandler(NodeEngine nodeEngine, Connection conn, long callId) {
            this.nodeEngine = nodeEngine;
            this.conn = conn;
            this.callId = callId;
        }

        public void sendResponse(Object obj) {
            if (!sent.compareAndSet(false, true)) {
                throw new ResponseAlreadySentException("Response already sent for call: " + callId
                                                + " to " + conn.getEndPoint() + ", current-response: " + obj);
            }
            final ResponseOperation responseOp = new ResponseOperation(obj);
            OperationAccessor.setCallId(responseOp, callId);
            nodeEngine.getOperationService().send(responseOp, conn);
        }
    }

    private static class LocalInvocationResponseHandler implements ResponseHandler {

        private final Callback<Object> callback;
        private final long callId;
        private final AtomicBoolean sent = new AtomicBoolean(false);

        private LocalInvocationResponseHandler(Callback<Object> callback, long callId) {
            this.callback = callback;
            this.callId = callId;
        }

        public void sendResponse(Object obj) {
            if (!sent.compareAndSet(false, true)) {
                throw new ResponseAlreadySentException("Response already sent for callback: " + callback
                        + ", current-response: : " + obj);
            }
            callback.notify(obj);
        }
    }

    private ResponseHandlerFactory() {
    }
}
