/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.nio.Connection;
import com.hazelcast.spi.OperationResponseHandler;

public final class OperationResponseHandlerFactory {

    private static final EmptyOperationResponseHandler EMPTY_RESPONSE_HANDLER = new EmptyOperationResponseHandler();

    private OperationResponseHandlerFactory() {
    }

    public static OperationResponseHandler createEmptyResponseHandler() {
        return EMPTY_RESPONSE_HANDLER;
    }

    public static class EmptyOperationResponseHandler implements OperationResponseHandler {
        @Override
        public void sendResponse(Connection receiver, boolean urgent, long callId, int backupCount, Object response) {
        }

        @Override
        public void sendErrorResponse(Connection receiver, boolean urgent, long callId, Throwable error) {
        }

        @Override
        public void sendTimeoutResponse(Connection receiver, boolean urgent, long callId) {
        }

        @Override
        public void sendBackupResponse(Address receiver, boolean urgent, long callId) {
        }

        @Override
        public boolean isLocal() {
            return false;
        }
    }

    public static OperationResponseHandler createErrorLoggingResponseHandler(ILogger logger) {
        return new ErrorLoggingResponseHandler(logger);
    }

    private static final class ErrorLoggingResponseHandler extends EmptyOperationResponseHandler {
        private final ILogger logger;

        private ErrorLoggingResponseHandler(ILogger logger) {
            this.logger = logger;
        }

        @Override
        public void sendErrorResponse(Connection receiver, boolean urgent, long callId, Throwable error) {
            logger.severe(error);
        }

        @Override
        public boolean isLocal() {
            return true;
        }
    }
}
