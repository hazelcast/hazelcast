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

package com.hazelcast.spi.impl;

import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Address;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationResponseHandler;

public final class OperationResponseHandlerFactory {

    private static final EmptyResponseHandler EMPTY_RESPONSE_HANDLER = new EmptyResponseHandler();

    private OperationResponseHandlerFactory() {
    }

    public static OperationResponseHandler createEmptyResponseHandler() {
        return EMPTY_RESPONSE_HANDLER;
    }

    private static class EmptyResponseHandler extends OperationResponseHandlerAdapter {

        // TODO: Should this not return true?
        @Override
        public boolean isLocal() {
            return false;
        }
    }

    public static OperationResponseHandler createErrorLoggingResponseHandler(ILogger logger) {
        return new ErrorLoggingResponseHandler(logger);
    }

    private static final class ErrorLoggingResponseHandler implements OperationResponseHandler {
        private final ILogger logger;

        private ErrorLoggingResponseHandler(ILogger logger) {
            this.logger = logger;
        }

        @Override
        public void sendNormalResponse(Operation op, Object response, int syncBackupCount) {
        }

        @Override
        public void sendBackupComplete(Address address, long callId, boolean urgent) {
        }

        @Override
        public void sendErrorResponse(Address address, long callId, boolean urgent, Operation op, Throwable cause) {
            //todo: should we not include operation information?
            logger.severe(cause);
        }

        @Override
        public void sendTimeoutResponse(Operation op) {
            //todo: should we not log the timeout?
        }

        @Override
        public boolean isLocal() {
            return true;
        }
    }
}
