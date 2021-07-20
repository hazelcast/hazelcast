/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.impl.operation;

import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.impl.execution.init.JetInitDataSerializerHook;
import com.hazelcast.spi.impl.AllowedDuringPassiveState;
import com.hazelcast.spi.impl.operationservice.UrgentSystemOperation;

import java.util.concurrent.CompletableFuture;

/**
 * Operation sent from a member that wants to gracefully shut down to the
 * master.
 * <p>
 * The master, for normal jobs, requests graceful termination of all
 * executions where the caller is participant (by sending a
 * TerminateExecutionOperation), except for those that have the {@link
 * JobConfig#isPreventShutdown()} flag enabled. It also prevents starting
 * of any new normal jobs, until the shutting-down member is actually
 * removed from the cluster.
 * <p>
 * For light jobs, the master sends {@link
 * NotifyShutdownToMembersOperation} to all members (including itself) to
 * deal with them (see that operation for description). This operation is
 * also sent to all newly-joining members.
 * <p>
 * It responds to the caller when all normal jobs have terminated and when
 * all responses for {@link NotifyShutdownToMembersOperation} are received.
 * <p>
 * Note that normal jobs can terminate in 3 ways:
 * <ol>
 *     <li>fault-tolerant job terminate after a terminal snapshot, regardless
 *     of the prevent-shutdown flag
 *
 *     <li>non-fault-tolerant jobs with the prevent-shutdown flag block
 *     response to this op until they complete normally
 *
 *     <li>non-fault-tolerant jobs without the prevent-shutdown flag are
 *     cancelled abruptly
 * </ol>
 *
 * If the operation fails, the caller will retry it indefinitely with the
 * new master.
 */
public class NotifyShutdownToMasterOperation extends AsyncOperation implements UrgentSystemOperation,
        AllowedDuringPassiveState {

    public NotifyShutdownToMasterOperation() {
    }

    @Override
    protected CompletableFuture<Void> doRun() {
        return getJobCoordinationService().addShuttingDownMember(getCallerUuid());
    }

    @Override
    public int getClassId() {
        return JetInitDataSerializerHook.NOTIFY_MEMBER_SHUTDOWN_P1_OP;
    }
}
