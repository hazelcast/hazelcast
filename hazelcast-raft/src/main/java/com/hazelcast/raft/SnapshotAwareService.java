/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.raft;

/**
 * The service interface required to be implemented by services that behave as Raft state machine.
 * Each Raft service must be able to generate a snapshot of its committed data and restore it back.
 *
 * @param <T> type of snapshot object
 */
public interface SnapshotAwareService<T> {

    /**
     * Creates a snapshot for specified {@link RaftGroupId}.
     *
     * @param groupId {@link RaftGroupId} which is snapshot requested for
     * @param commitIndex commitIndex of the Raft state when the snapshot is requested
     * @return snapshot for specified {@link RaftGroupId}.
     */
    T takeSnapshot(RaftGroupId groupId, long commitIndex);

    /**
     * Restores the snapshot for specified {@link RaftGroupId}.
     *
     * @param groupId {@link RaftGroupId} of the snapshot to be restored
     * @param commitIndex commitIndex of the Raft state when snapshot is created
     * @param snapshot snapshot for specified {@link RaftGroupId}
     */
    void restoreSnapshot(RaftGroupId groupId, long commitIndex, T snapshot);

}
