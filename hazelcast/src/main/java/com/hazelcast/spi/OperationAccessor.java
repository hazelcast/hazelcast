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

import com.hazelcast.cluster.impl.operations.JoinOperation;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.Connection;
import com.hazelcast.partition.MigrationCycleOperation;
import com.hazelcast.spi.annotation.PrivateApi;

/**
 * Allows access to certain attributes on an Operation.
 */
@PrivateApi
public final class OperationAccessor {

    private static final ClassLoader THIS_CLASS_LOADER = OperationAccessor.class.getClassLoader();

    private OperationAccessor() {
    }

    public static boolean isJoinOperation(Operation op) {
        return op instanceof JoinOperation
                && op.getClass().getClassLoader() == THIS_CLASS_LOADER;
    }

    public static boolean isMigrationOperation(Operation op) {
        return op instanceof MigrationCycleOperation
                && op.getClass().getClassLoader() == THIS_CLASS_LOADER;
    }

    public static void setCallerAddress(Operation op, Address caller) {
        op.setCallerAddress(caller);
    }

    public static void setConnection(Operation op, Connection connection) {
        op.setConnection(connection);
    }

    /**
     * Sets the callId.
     *
     * @param op the Operator to update
     * @param callId the callId.
     * @see Operation#setCallId(long)
     * @see Operation#getCallId()
     */
    public static void setCallId(Operation op, long callId) {
        op.setCallId(callId);
    }

    /**
     * Sets the invocation time.
     *
     * @param op the Operation to update.
     * @param invocationTime the new invocation time.
     * @see Operation#setInvocationTime(long)
     */
    public static void setInvocationTime(Operation op, long invocationTime) {
        op.setInvocationTime(invocationTime);
    }

    /**
     * Sets the call timeout in ms.
     *
     * @param op the Operation to update.
     * @param callTimeout the call timeout in ms.
     * @see com.hazelcast.spi.Operation#setCallTimeout(long)
     */
    public static void setCallTimeout(Operation op, long callTimeout) {
        op.setCallTimeout(callTimeout);
    }

}
