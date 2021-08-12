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

import com.hazelcast.internal.cluster.impl.operations.JoinOperation;
import com.hazelcast.jet.impl.JetServiceBackend;
import com.hazelcast.jet.impl.JobCoordinationService;
import com.hazelcast.jet.impl.execution.init.JetInitDataSerializerHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.impl.AllowedDuringPassiveState;
import com.hazelcast.spi.impl.operationservice.UrgentSystemOperation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static com.hazelcast.jet.impl.util.NamedCompletableFuture.loggedAllOf;

/**
 * The operation is sent from the master to all members informing them
 * about a member that wants to gracefully shut down. Secondly, it's sent
 * as a pre-join op to newly-joining members.
 * <p>
 * The operation has to be idempotent: it can be sent as a part of pre-join
 * op, and again as a part of sending to all current members. It can be
 * also sent again from a new master after the old master failure.
 * <p>
 * See {@link JobCoordinationService#addShuttingDownMember(UUID)} for more
 * information.
 * <p>
 * This operation doesn't deal at all with normal jobs.
 */
public class NotifyShutdownToMemberOperation extends AsyncOperation implements UrgentSystemOperation,
        AllowedDuringPassiveState, JoinOperation {

    private Collection<UUID> shuttingDownMemberIds;

    public NotifyShutdownToMemberOperation() {
    }

    public NotifyShutdownToMemberOperation(Collection<UUID> shuttingDownMemberIds) {
        this.shuttingDownMemberIds = shuttingDownMemberIds;
    }

    @Override
    protected CompletableFuture<Void> doRun() {
        List<CompletableFuture<?>> futures = new ArrayList<>();
        // add shutting-down members
        for (UUID uuid : shuttingDownMemberIds) {
            futures.add(getJobCoordinationService().addShuttingDownMember(uuid));
            // handle SQL client cursors
            futures.add(getNodeEngine().getSqlService().getInternalService().getClientStateRegistry()
                    .onMemberGracefulShutdown(uuid));
        }
        return loggedAllOf(getLogger(), "NotifyShutdownToMemberOperation-" + shuttingDownMemberIds, futures.toArray(new CompletableFuture[0]))
                .whenComplete((r, t) -> getLogger().info("aaa NotifyShutdownToMemberOperation-" + shuttingDownMemberIds + " completed")); // TODO [viliam] remove logging
    }

    @Override
    public String getServiceName() {
        return JetServiceBackend.SERVICE_NAME;
    }

    @Override
    public int getClassId() {
        return JetInitDataSerializerHook.NOTIFY_MEMBER_SHUTDOWN_TO_MEMBERS_OP;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeObject(shuttingDownMemberIds);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        shuttingDownMemberIds = in.readObject();
    }
}
