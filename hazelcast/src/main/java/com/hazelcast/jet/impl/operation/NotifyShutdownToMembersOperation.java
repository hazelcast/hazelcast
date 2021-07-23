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
import com.hazelcast.jet.impl.JetServiceBackend;
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

/**
 * The operation is sent from the master to all members informing them
 * about a member that wants to gracefully shut down. It's also sent as a
 * pre-join op to newly-joining members.
 * <p>
 * The operation has to be idempotent: it can be sent as a part of pre-join
 * op, and again as a part of sending to all current members. It can be
 * also sent again from a new master after the old master failure.
 * <p>
 * After receiving, the member ensures no new light job uses the
 * shutting-down member as a participant. For current light jobs that have
 * the shutting-down member as a participant, it cancels those without the
 * {@link JobConfig#isPreventShutdown()} flag enabled immediately. Those
 * with the prevent-shutdown flag enabled are allowed to continue. The
 * operation responds to the caller, when all light jobs coordinated by
 * this member terminated.
 * <p>
 * This operation doesn't deal at all with normal jobs.
 */
public class NotifyShutdownToMembersOperation extends AsyncOperation implements UrgentSystemOperation,
        AllowedDuringPassiveState {

    private Collection<UUID> shuttingDownMemberIds;

    public NotifyShutdownToMembersOperation() {
    }

    public NotifyShutdownToMembersOperation(Collection<UUID> shuttingDownMemberIds) {
        this.shuttingDownMemberIds = shuttingDownMemberIds;
    }

    @Override
    protected CompletableFuture<Void> doRun() {
        List<CompletableFuture<?>> futures = new ArrayList<>();
        for (UUID uuid : shuttingDownMemberIds) {
            futures.add(getJobCoordinationService().addShuttingDownMember(uuid));
        }
        return CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]));
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
