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

import com.hazelcast.cp.internal.RaftGroupId;
import com.hazelcast.cp.internal.raft.impl.persistence.LogFileStructure;
import com.hazelcast.cp.internal.raft.impl.persistence.NopRaftStateStore;
import com.hazelcast.cp.internal.raft.impl.persistence.RaftStateStore;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Used when CP Subsystem works transiently (its state is not persisted).
 */
public final class NopCPPersistenceService implements CPPersistenceService {

    public static final CPPersistenceService INSTANCE = new NopCPPersistenceService();

    @Override
    public boolean isEnabled() {
        return false;
    }

    @Override
    public CPMetadataStore getCPMetadataStore() {
        return NopCPMetadataStore.INSTANCE;
    }

    @Override
    public RaftStateStore createRaftStateStore(@Nonnull RaftGroupId groupId, @Nullable LogFileStructure logFileStructure) {
        return NopRaftStateStore.INSTANCE;
    }

    @Override
    public void removeRaftStateStore(@Nonnull RaftGroupId groupId) {
    }

    @Override
    public void reset() {
    }
}
