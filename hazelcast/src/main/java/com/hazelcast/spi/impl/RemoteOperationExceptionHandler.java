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
import com.hazelcast.logging.Logger;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.Connection;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.ResponseHandler;
import com.hazelcast.spi.exception.RetryableException;

import java.util.logging.Level;

/**
 * This class is used to handle deserialization exceptions on remote nodes
 * to notify the actual caller about the problem. The original caller operation
 * will not timeout but present the remote exception.
 */
class RemoteOperationExceptionHandler
        implements RemotePropagatable<RemoteOperationExceptionHandler> {

    private ResponseHandler responseHandler;
    private Address callerAddress;
    private Connection connection;
    private NodeEngine nodeEngine;

    private long callId;

    RemoteOperationExceptionHandler() {
    }

    RemoteOperationExceptionHandler(long callId) {
        this.callId = callId;
    }

    @Override
    public ResponseHandler getResponseHandler() {
        return responseHandler;
    }

    @Override
    public RemoteOperationExceptionHandler setResponseHandler(ResponseHandler responseHandler) {
        this.responseHandler = responseHandler;
        return this;
    }

    public long getCallId() {
        return callId;
    }

    @Override
    public boolean returnsResponse() {
        return callId != 0;
    }

    @Override
    public Connection getConnection() {
        return connection;
    }

    @Override
    public boolean isUrgent() {
        return true;
    }

    @Override
    public Address getCallerAddress() {
        return callerAddress;
    }

    @Override
    public void logError(Throwable t) {
        final ILogger logger = Logger.getLogger(RemoteOperationExceptionHandler.class);
        if (t instanceof RetryableException) {
            final Level level = returnsResponse() ? Level.FINEST : Level.WARNING;
            if (logger.isLoggable(level)) {
                logger.log(level, t.getClass().getName() + ": " + t.getMessage());
            }
        } else if (t instanceof OutOfMemoryError) {
            try {
                logger.log(Level.SEVERE, t.getMessage(), t);
            } catch (Throwable ignored) {
                logger.log(Level.SEVERE, ignored.getMessage(), t);
            }
        } else {
            final Level level = nodeEngine != null && nodeEngine.isActive() ? Level.SEVERE : Level.FINEST;
            if (logger.isLoggable(level)) {
                logger.log(level, t.getMessage(), t);
            }
        }
    }

    void setCallerAddress(Address callerAddress) {
        this.callerAddress = callerAddress;
    }

    void setConnection(Connection connection) {
        this.connection = connection;
    }

    void setNodeEngine(NodeEngine nodeEngine) {
        this.nodeEngine = nodeEngine;
    }
}

