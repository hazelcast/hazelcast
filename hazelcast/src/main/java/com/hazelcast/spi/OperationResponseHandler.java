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

package com.hazelcast.spi;

import com.hazelcast.nio.Address;

/**
 * A handler for the {@link com.hazelcast.spi.OperationService} when it has calculated a response. This way you can hook
 * into the Operation execution and decide what to do with it: for example, send it to the right machine.
 *
 * There are 2 difference between the {@link ResponseHandler} and the OperationResponseHandler:
 * <ol>
 *     <li>the OperationResponseHandler can be re-used since it isn't tied to a particular execution.</li>
 *     <li>the OperationResponseHandler has specific methods for types of responses instead of a generic one.
 *     This prevent forcing to create {@link com.hazelcast.spi.impl.operationservice.impl.responses.Response} instances.</li>
 * </ol>
 *
 * Also during the development of Hazelcast 3.6 additional methods will be added to the OperationResponseHandler for certain
 * types of responses like exceptions, backup complete etc.
 */
public interface OperationResponseHandler {

    /**
     * Sends back a normal response
     *
     * @param op
     * @param response
     * @param syncBackupCount send back to the caller for how many acks of sync backups he needs to wait for.
     */
    void sendNormalResponse(Operation op, Object response, int syncBackupCount);

    void sendBackupComplete(Address address, long callId, boolean urgent);

    /**
     * Sends a error response.
     *
     * @param address
     * @param callId
     * @param urgent
     * @param op the Operation that failed. The Operation could be null e.g. if the operation could not be deserialized due
     *           to a deserialization error.
     * @param cause
     */
    void sendErrorResponse(Address address, long callId, boolean urgent, Operation op, Throwable cause);

    void sendTimeoutResponse(Operation op);

    /**
     * Checks if this OperationResponseHandler is for a local invocation.
     *
     * @return true if local, false otherwise.
     */
    boolean isLocal();
}
