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

package com.hazelcast.cp.internal.operation.unsafe;

import com.hazelcast.cp.CPGroupId;
import com.hazelcast.cp.internal.RaftOp;
import com.hazelcast.cp.internal.RaftService;
import com.hazelcast.cp.internal.RaftServiceDataSerializerHook;
import com.hazelcast.cp.internal.raft.impl.util.PostponedResponse;
import com.hazelcast.spi.impl.operationservice.BackupAwareOperation;
import com.hazelcast.spi.impl.operationservice.CallStatus;
import com.hazelcast.spi.impl.operationservice.Operation;

/**
 * Partition-based Raft replicate operation for Raft services to be executed in UNSAFE mode.
 * This operation wraps original {@link RaftOp} and executes it as a replication operation
 * outside of Raft algorithm.
 */
public class UnsafeRaftReplicateOp extends AbstractUnsafeRaftOp implements BackupAwareOperation {

    public UnsafeRaftReplicateOp() {
    }

    public UnsafeRaftReplicateOp(CPGroupId groupId, RaftOp op) {
        super(groupId, op);
    }

    @Override
    CallStatus handleResponse(long commitIndex, Object response) {
        if (response == PostponedResponse.INSTANCE) {
            RaftService service = getService();
            service.registerUnsafeWaitingOperation(groupId, commitIndex, this);
            return CallStatus.VOID;
        }
        return CallStatus.RESPONSE;
    }

    @Override
    public boolean shouldBackup() {
        return true;
    }

    @Override
    public int getSyncBackupCount() {
        return 1;
    }

    @Override
    public int getAsyncBackupCount() {
        return 0;
    }

    @Override
    public Operation getBackupOperation() {
        return new UnsafeRaftBackupOp(groupId, op);
    }

    @Override
    public int getClassId() {
        return RaftServiceDataSerializerHook.UNSAFE_RAFT_REPLICATE_OP;
    }
}
