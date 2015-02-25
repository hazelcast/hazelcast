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

import com.hazelcast.instance.MemberImpl;
import com.hazelcast.nio.Packet;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationService;

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
     * Executes a Runnable on a thread that is responsible for a given partition.
     *
     * This method is typically used by the {@link com.hazelcast.client.ClientEngine} when it has received a Packet containing
     * a request that needs to be processed. The advantage of this method is that the request can immediately be handed over to
     * a thread that can take care of it; either execute it directly or send it to the remote machine.
     *
     * @param task the task to execute
     * @param partitionId the partition id. A partition of smaller than 0, means that the task is going to be executed in
     *                    the generic operation-threads and not on a partition specific operation-thread.
     */
    void execute(Runnable task, int partitionId);

    /**
     * Executes an operation.
     *
     * This method is typically called by the IO system when an operation-packet is received.
     *
     * @param packet the packet containing the serialized operation.
     */
    void executeOperation(Packet packet);

    /**
     * Resets internal state of InternalOperationService.
     * Notifies registered invocations with an error message.
     */
    void reset();

    /**
     * Shuts down this InternalOperationService.
     */
    void shutdown();
}
