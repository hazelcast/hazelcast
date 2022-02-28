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

import com.hazelcast.cluster.Address;
import com.hazelcast.internal.server.ServerConnection;
import com.hazelcast.spi.annotation.PrivateApi;

/**
 * Allows access to certain attributes on an Operation.
 */
@PrivateApi
public final class OperationAccessor {

    private OperationAccessor() {
    }

    public static void setCallerAddress(Operation op, Address caller) {
        op.setCallerAddress(caller);
    }

    public static void setConnection(Operation op, ServerConnection connection) {
        op.setConnection(connection);
    }

    /**
     * Assigns the supplied call ID to the supplied operation, thereby activating it.
     * Refer to Operation#setCallId(long) and Operation#getCallId() for detailed semantics.
     */
    public static void setCallId(Operation op, long callId) {
        op.setCallId(callId);
    }

    /**
     * Marks the supplied operation as "not active". Refer to {@link Operation#deactivate()} for
     * detailed semantics.
     */
    public static boolean deactivate(Operation op) {
        return op.deactivate();
    }

    public static boolean hasActiveInvocation(Operation op) {
        return op.isActive();
    }

    /**
     * Sets the invocation time for the Operation.
     *
     * @param op the Operation that is updated for its invocation time.
     * @param invocationTime the new invocation time.
     * @see Operation#setInvocationTime(long)
     */
    public static void setInvocationTime(Operation op, long invocationTime) {
        op.setInvocationTime(invocationTime);
    }

    /**
     * Sets the call timeout in milliseconds for the Operation.
     *
     * @param op the Operation to updated for its call timeout.
     * @param callTimeout the call timeout in ms.
     * @see Operation#setCallTimeout(long)
     */
    public static void setCallTimeout(Operation op, long callTimeout) {
        op.setCallTimeout(callTimeout);
    }

}
