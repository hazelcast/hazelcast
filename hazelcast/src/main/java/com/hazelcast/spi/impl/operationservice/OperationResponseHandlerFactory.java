/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.spi.impl.operationservice;

import com.hazelcast.logging.ILogger;

public final class OperationResponseHandlerFactory {

    private static final NoResponseHandler EMPTY_RESPONSE_HANDLER = new NoResponseHandler();

    private OperationResponseHandlerFactory() {
    }

    public static OperationResponseHandler createEmptyResponseHandler() {
        return EMPTY_RESPONSE_HANDLER;
    }

    private static class NoResponseHandler
            implements OperationResponseHandler {

        @Override
        public void sendResponse(Operation op, Object obj) {
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
        public void sendResponse(Operation op, Object obj) {
            if (obj instanceof Throwable) {
                Throwable t = (Throwable) obj;
                logger.severe(t);
            }
        }
    }
}
