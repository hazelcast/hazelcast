/*
 * Copyright (c) 2008-2024, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.cp;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

/**
 * Represents an immutable snapshot of the current CP groups view
 * from 1 member of the Hazelcast cluster. This snapshot object is
 * used to share views between members and clients.
 */
public class CPGroupsSnapshot {
    /** An empty snapshot for use when there is no data to provide */
    public static final CPGroupsSnapshot EMPTY = new CPGroupsSnapshot(Collections.emptyMap(), Collections.emptyMap());

    private final Map<CPGroupId, GroupInfo> groupIdToInfo;
    private final Map<UUID, UUID> cpToApUuids;

    /**
     * Create a new {@link CPGroupsSnapshot} with included CP to AP UUID mapping. This means
     * AP UUIDs can be looked up directly from this snapshot without needing to query the
     * cluster service.
     *
     * @param groupIdToInfo mapping of {@link CPGroupId} to {@link GroupInfo}
     * @param cpToApUuids   mapping of last known CP {@link UUID} to AP {@link UUID} for members in this group
     */
    public CPGroupsSnapshot(Map<CPGroupId, GroupInfo> groupIdToInfo, Map<UUID, UUID> cpToApUuids) {
        this.groupIdToInfo = groupIdToInfo;
        this.cpToApUuids = cpToApUuids;
    }

    /**
     * Create a new {@link CPGroupsSnapshot} without any CP to AP UUID mapping. This means
     * AP UUIDs will need to be looked up separately if they are required.
     *
     * @param groupIdToInfo mapping of {@link CPGroupId} to {@link GroupInfo}
     */
    public CPGroupsSnapshot(Map<CPGroupId, GroupInfo> groupIdToInfo) {
        this.groupIdToInfo = groupIdToInfo;
        this.cpToApUuids = Collections.emptyMap();
    }

    public Map<CPGroupId, GroupInfo> getAllGroupInformation() {
        return groupIdToInfo;
    }

    public CPMember getLeader(CPGroupId groupId) {
        GroupInfo info = groupIdToInfo.get(groupId);
        return info == null ? null : info.leader();
    }

    public Map<UUID, UUID> getCpToApUuids() {
        return cpToApUuids;
    }

    public UUID getApUuid(CPMember cpMember) {
        return cpToApUuids.get(cpMember.getUuid());
    }

    public record GroupInfo(CPMember leader, Set<CPMember> followers, int term) {

    }
}
