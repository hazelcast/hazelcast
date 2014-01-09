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

import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.*;

/**
 * @author mdogan 8/2/12
 */
public final class ResponseHandlerFactory {

   public static final NoResponseHandler NO_RESPONSE_HANDLER = new NoResponseHandler();

    public static ResponseHandler createEmptyResponseHandler() {
        return NO_RESPONSE_HANDLER;
    }

    private static class NoResponseHandler implements ResponseHandler {
        public void sendResponse(final Object obj) {
        }

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

        public void sendResponse(final Object obj) {
            if (obj instanceof Throwable) {
                Throwable t = (Throwable) obj;
                logger.severe(t);
            }
        }

        public boolean isLocal() {
            return true;
        }
    }

    private ResponseHandlerFactory() {
    }
}
