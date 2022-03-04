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

package com.hazelcast.cp.internal.persistence;

import com.hazelcast.cp.CPMember;
import com.hazelcast.cp.internal.RaftGroupId;
import com.hazelcast.cp.internal.raft.impl.persistence.LogFileStructure;
import com.hazelcast.cp.internal.raft.impl.persistence.RaftStateStore;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Defines the contract for providing persistence utilities to CP Subsystem
 */
public interface CPPersistenceService {

    /**
     * Returns if the CP state persistence is enabled or not.
     */
    boolean isEnabled();

    /**
     * Returns the utility that is used for storing {@link CPMember} identity of
     * the local Hazelcast member.
     */
    CPMetadataStore getCPMetadataStore();

    /**
     * Creates a {@link RaftStateStore} that is going to be used by the local Raft node
     * of the given CP group.
     */
    RaftStateStore createRaftStateStore(@Nonnull RaftGroupId groupId, @Nullable LogFileStructure logFileStructure);

    /**
     * Removes the store associated with groupId,
     * removes all persisted state and releases acquired resources.
     */
    void removeRaftStateStore(@Nonnull RaftGroupId groupId);

    /**
     * Deletes all persisted data from the storage.
     */
    void reset();
}
