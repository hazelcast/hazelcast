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

package com.hazelcast.cp.internal.raft;

import com.hazelcast.cp.CPGroupId;

/**
 * The service interface required to be implemented by services that behave as
 * a Raft state machine. Each Raft service must be able to generate a snapshot
 * of its committed data and restore it back.
 *
 * @param <T> type of snapshot object
 */
public interface SnapshotAwareService<T> {

    /**
     * Creates a snapshot for specified {@link CPGroupId}.
     * The snapshot object returned from this method will be stored among
     * the Raft log and it can be sent to other Raft nodes anytime. Therefore,
     * this method must not return a snapshot object that can mutate after
     * the takeSnapshot() call.
     *
     * @param groupId {@link CPGroupId} which is snapshot requested for
     * @param commitIndex commitIndex of the taken snapshot
     * @return snapshot for specified {@link CPGroupId}.
     */
    T takeSnapshot(CPGroupId groupId, long commitIndex);

    /**
     * Restores the snapshot for specified {@link CPGroupId}.
     *
     * @param groupId {@link CPGroupId} of the snapshot to be restored
     * @param commitIndex commitIndex of the restored snapshot
     * @param snapshot snapshot for specified {@link CPGroupId}
     */
    void restoreSnapshot(CPGroupId groupId, long commitIndex, T snapshot);

}
