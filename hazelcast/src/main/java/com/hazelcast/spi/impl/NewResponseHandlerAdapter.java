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

import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationResponseHandler;
import com.hazelcast.spi.ResponseHandler;
import com.hazelcast.spi.impl.operationservice.impl.responses.CallTimeoutResponse;
import com.hazelcast.spi.impl.operationservice.impl.responses.ErrorResponse;
import com.hazelcast.spi.impl.operationservice.impl.responses.NormalResponse;
import com.hazelcast.spi.impl.operationservice.impl.responses.Response;

/**
 * A {@link ResponseHandler} adapter that adapts a new {@link OperationResponseHandler} to behave like a
 * old {@link ResponseHandler}.
 */
@Deprecated
public class NewResponseHandlerAdapter implements ResponseHandler {

    private final OperationResponseHandler handler;
    private final Operation op;

    public NewResponseHandlerAdapter(OperationResponseHandler handler, Operation op) {
        this.handler = handler;
        this.op = op;
    }

    @Override
    public void sendResponse(Object obj) {
        if (obj instanceof Response) {
            if (obj instanceof NormalResponse) {
                NormalResponse normalResponse = (NormalResponse) obj;
                handler.sendNormalResponse(op, normalResponse.getValue(), normalResponse.getBackupCount());
            } else if (obj instanceof ErrorResponse) {
                ErrorResponse errorResponse = (ErrorResponse) obj;
                handler.sendErrorResponse(op.getCallerAddress(), op.getCallId(), op.isUrgent(), op, errorResponse.getCause());
            } else if (obj instanceof CallTimeoutResponse) {
                handler.sendTimeoutResponse(op);
            } else {
                // other responses like backup response will never been send through the old response handler.
                throw new IllegalArgumentException("Unhandled response:" + obj);
            }
        } else if (obj instanceof Throwable) {
            handler.sendErrorResponse(op.getCallerAddress(), op.getCallId(), op.isUrgent(), op, (Throwable) obj);
        } else {
            handler.sendNormalResponse(op, obj, 0);
        }
    }

    @Override
    public boolean isLocal() {
        return handler.isLocal();
    }
}
