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

package com.hazelcast.cp.internal;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.impl.operationservice.Operation;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * State for {@linkplain RaftService} used in UNSAFE mode.
 * UnsafeModePartitionState keeps track of {@code commitIndex}
 * and registrations of waiting operations, such as {@code FencedLock.lock()}.
 * <p>
 * UnsafeModePartitionState is replicated during migration
 * with {@linkplain com.hazelcast.cp.internal.operation.unsafe.UnsafeStateReplicationOp UnsafeStateReplicationOp}.
 */
public class UnsafeModePartitionState implements IdentifiedDataSerializable {

    private long commitIndex;
    private final transient Map<Long, Operation> waitingOperations = new HashMap<>();

    long nextCommitIndex() {
        return ++commitIndex;
    }

    long commitIndex() {
        return commitIndex;
    }

    boolean registerWaitingOp(long commitIndex, Operation op) {
        return waitingOperations.putIfAbsent(commitIndex, op) == null;
    }

    Operation removeWaitingOp(long index) {
        return waitingOperations.remove(index);
    }

    Collection<Operation> getWaitingOps() {
        return waitingOperations.values();
    }

    void apply(UnsafeModePartitionState state) {
        commitIndex = state.commitIndex;
        waitingOperations.clear();
        waitingOperations.putAll(state.waitingOperations);
    }

    void reset() {
        commitIndex = 0;
        waitingOperations.clear();
    }

    @Override
    public int getFactoryId() {
        return RaftServiceDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return RaftServiceDataSerializerHook.UNSAFE_MODE_PARTITION_STATE;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeLong(commitIndex);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        commitIndex = in.readLong();
    }
}
