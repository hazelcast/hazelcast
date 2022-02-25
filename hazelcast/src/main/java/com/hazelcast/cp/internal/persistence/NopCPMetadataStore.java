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

import java.util.Collection;

/**
 * Used when CP Subsystem works transiently (its state is not persisted).
 */
public final class NopCPMetadataStore implements CPMetadataStore {

    public static final CPMetadataStore INSTANCE = new NopCPMetadataStore();

    private NopCPMetadataStore() {
    }

    @Override
    public boolean isMarkedAPMember() {
        return false;
    }

    @Override
    public boolean tryMarkAPMember() {
        return true;
    }

    @Override
    public boolean containsLocalMemberFile() {
        return false;
    }

    @Override
    public void persistLocalCPMember(CPMember member) {
    }

    @Override
    public CPMember readLocalCPMember() {
        return null;
    }

    @Override
    public void persistActiveCPMembers(Collection<? extends CPMember> members, long commitIndex) {
    }

    @Override
    public long readActiveCPMembers(Collection<CPMember> members) {
        return 0;
    }

    @Override
    public void persistMetadataGroupId(RaftGroupId groupId) {
    }

    @Override
    public RaftGroupId readMetadataGroupId() {
        return null;
    }

}
