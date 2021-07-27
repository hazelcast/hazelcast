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

import com.hazelcast.cluster.Member;
import com.hazelcast.jet.impl.JetServiceBackend;
import com.hazelcast.jet.impl.JobCoordinationService;
import com.hazelcast.jet.impl.execution.init.JetInitDataSerializerHook;
import com.hazelcast.spi.impl.AllowedDuringPassiveState;
import com.hazelcast.spi.impl.operationservice.UrgentSystemOperation;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static java.util.Collections.singletonList;

/**
 * A member that wants to gracefully shut down sends this operation to the
 * master. Master sends it to itself.
 * <p>
 * The operation calls {@link
 * JobCoordinationService#addShuttingDownMember(UUID)} with the caller
 * UUID. It also forwards the information to all members using {@link
 * NotifyShutdownToMembersOperation}.
 * <p>
 * It responds to the caller when all executions have terminated and when
 * all responses for {@link NotifyShutdownToMembersOperation} are received.
 * <p>
 * If the operation fails, the caller will retry it indefinitely with the
 * new master. The operation has to be idempotent.
 */
public class NotifyShutdownToMasterOperation extends AsyncOperation implements UrgentSystemOperation,
        AllowedDuringPassiveState {

    public NotifyShutdownToMasterOperation() {
    }

    @Override
    protected CompletableFuture<Void> doRun() {
        List<CompletableFuture<?>> futures = new ArrayList<>();

        // handle jobs locally
        futures.add(getJobCoordinationService().addShuttingDownMember(getCallerUuid()));

        // handle SQL client cursors locally
        futures.add(getNodeEngine().getSqlService().getInternalService().getClientStateRegistry()
                .completionFutureForCurrentCursors());

        // forward to the rest of the cluster
        UUID localMemberUuid = getNodeEngine().getLocalMember().getUuid();
        for (Member member : getNodeEngine().getClusterService().getMembers()) {
            if (member.getUuid().equals(localMemberUuid)) {
                continue;
            }
            NotifyShutdownToMembersOperation op = new NotifyShutdownToMembersOperation(singletonList(getCallerUuid()));
            CompletableFuture<Object> future = getNodeEngine().getOperationService().
                    invokeOnTarget(JetServiceBackend.SERVICE_NAME, op, member.getAddress());
            future = future.whenComplete((r, t) -> {
                if (t != null) {
                    getLogger().info("aaa returned from " + member.getAddress() + ", t=" + t, t);
                }
            });
            futures.add(future);
        }

        // done when all are done
        // TODO [viliam] handle the timeout
        return CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]));
    }

    @Override
    public int getClassId() {
        return JetInitDataSerializerHook.NOTIFY_MEMBER_SHUTDOWN_TO_MASTER_OP;
    }
}
