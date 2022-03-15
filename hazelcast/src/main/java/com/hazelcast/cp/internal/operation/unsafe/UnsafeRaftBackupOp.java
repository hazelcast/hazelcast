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
import com.hazelcast.cp.internal.RaftServiceDataSerializerHook;
import com.hazelcast.spi.impl.operationservice.BackupOperation;
import com.hazelcast.spi.impl.operationservice.CallStatus;

/**
 * Partition-based backup operation for Raft services to be executed in UNSAFE mode.
 * This operation is the backup operation for {@link UnsafeRaftReplicateOp}.
 */
public class UnsafeRaftBackupOp extends AbstractUnsafeRaftOp implements BackupOperation {

    public UnsafeRaftBackupOp() {
    }

    public UnsafeRaftBackupOp(CPGroupId groupId, RaftOp op) {
        super(groupId, op);
    }

    @Override
    CallStatus handleResponse(long commitIndex, Object response) {
        return CallStatus.VOID;
    }

    @Override
    public int getClassId() {
        return RaftServiceDataSerializerHook.UNSAFE_RAFT_BACKUP_OP;
    }
}
