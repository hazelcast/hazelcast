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

import com.hazelcast.instance.MemberImpl;
import com.hazelcast.nio.Address;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationService;
import com.hazelcast.spi.impl.operationexecutor.OperationExecutor;

/**
 * This is the interface that needs to be implemented by actual InternalOperationService. Currently there is a single
 * InternalOperationService: {@link com.hazelcast.spi.impl.BasicOperationService}, but in the future others can be added.
 * <p/>
 * It exposes methods that will not be called by regular code, like shutdown, but will only be called by
 * the the SPI management.
 */
public interface InternalOperationService extends OperationService {

    void onMemberLeft(MemberImpl member);

    boolean isCallTimedOut(Operation op);

    void notifyBackupCall(long callId);

    /**
     * Executes a PartitionSpecificRunnable.
     *
     * This method is typically used by the {@link com.hazelcast.client.ClientEngine} when it has received a Packet containing
     * a request that needs to be processed.
     *
     * @param task the task to execute
     */
    void execute(PartitionSpecificRunnable task);

    /**
     * Sends a response to a remote machine.
     *
     * This method is deprecated since 3.5. It is an implementation detail.
     *
     * @param response the response to send.
     * @param target   the address of the target machine
     * @return true if send is successful, false otherwise.
     */
    boolean send(Response response, Address target);

    OperationExecutor getOperationExecutor();

    boolean isOperationExecuting(Address callerAddress, String callerUuid, String serviceName, Object identifier);

    boolean isOperationExecuting(Address callerAddress, int partitionId, long operationCallId);

    /**
     * Shuts down this InternalOperationService.
     */
    void shutdown();
}
