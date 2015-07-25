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

import com.hazelcast.nio.Address;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationResponseHandler;
import com.hazelcast.spi.ResponseHandler;
import com.hazelcast.spi.impl.operationservice.impl.responses.BackupResponse;
import com.hazelcast.spi.impl.operationservice.impl.responses.CallTimeoutResponse;
import com.hazelcast.spi.impl.operationservice.impl.responses.ErrorResponse;
import com.hazelcast.spi.impl.operationservice.impl.responses.NormalResponse;

/**
 * An adapter for an old {@link ResponseHandler} to act like a new OperationResponseHandler.
 *
 * This code will be removed as soon as the {@link ResponseHandler} is removed.
 */
@Deprecated
public class OldResponseHandlerAdapter implements OperationResponseHandler {

    private final ResponseHandler responseHandler;

    public OldResponseHandlerAdapter(ResponseHandler responseHandler) {
        this.responseHandler = responseHandler;
    }

    public ResponseHandler getResponseHandler() {
        return responseHandler;
    }

    @Override
    public void sendNormalResponse(Operation op, Object response, int syncBackupCount) {
        if (syncBackupCount == 0) {
            responseHandler.sendResponse(response);
        } else {
            responseHandler.sendResponse(new NormalResponse(response, op.getCallId(), syncBackupCount, op.isUrgent()));
        }
    }

    @Override
    public void sendBackupComplete(Address address, long callId, boolean urgent) {
        responseHandler.sendResponse(new BackupResponse(callId, urgent));
    }

    @Override
    public void sendErrorResponse(Address address, long callId, boolean urgent, Operation op, Throwable cause) {
        responseHandler.sendResponse(new ErrorResponse(cause, callId, urgent));
    }

    @Override
    public void sendTimeoutResponse(Operation op) {
        responseHandler.sendResponse(new CallTimeoutResponse(op.getCallId(), op.isUrgent()));
    }

    @Override
    public boolean isLocal() {
        return responseHandler.isLocal();
    }
}
