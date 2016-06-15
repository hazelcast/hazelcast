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

package com.hazelcast.spi.impl.operationservice.impl;

import com.hazelcast.nio.Connection;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationResponseHandler;
import com.hazelcast.spi.impl.operationservice.impl.responses.ErrorResponse;
import com.hazelcast.spi.impl.operationservice.impl.responses.NormalResponse;
import com.hazelcast.spi.impl.operationservice.impl.responses.Response;

/**
 * An {@link OperationResponseHandler} that is used for a remotely executed Operation. So when a calling member
 * sends an Operation to the receiving member, the receiving member attaches this RemoteInvocationResponseHandler
 * to that operation.
 */
final class RemoteInvocationResponseHandler implements OperationResponseHandler {
    private final OperationServiceImpl operationService;

    RemoteInvocationResponseHandler(OperationServiceImpl operationService) {
        this.operationService = operationService;
    }

    @Override
    public void sendResponse(Operation operation, Object obj) {
        Connection conn = operation.getConnection();

        Response response;
        if (obj instanceof Throwable) {
            response = new ErrorResponse((Throwable) obj, operation.getCallId(), operation.isUrgent());
        } else if (!(obj instanceof Response)) {
            response = new NormalResponse(obj, operation.getCallId(), 0, operation.isUrgent());
        } else {
            response = (Response) obj;
        }

        if (!operationService.send(response, operation.getCallerAddress())) {
            operationService.logger.warning("Cannot send response: " + obj + " to " + conn.getEndPoint()
                    + ". " + operation);
        }
    }

    @Override
    public boolean isLocal() {
        return false;
    }
}
