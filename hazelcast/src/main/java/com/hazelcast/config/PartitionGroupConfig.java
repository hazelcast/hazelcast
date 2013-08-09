/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.config;

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

public class PartitionGroupConfig {

    private boolean enabled = false;

    private MemberGroupType groupType = MemberGroupType.PER_MEMBER;

    private final List<MemberGroupConfig> memberGroupConfigs = new LinkedList<MemberGroupConfig>();

    public enum MemberGroupType {
        HOST_AWARE, CUSTOM, PER_MEMBER
    }

    public boolean isEnabled() {
        return enabled;
    }

    public PartitionGroupConfig setEnabled(final boolean enabled) {
        this.enabled = enabled;
        return this;
    }

    public MemberGroupType getGroupType() {
        return groupType;
    }

    public PartitionGroupConfig setGroupType(MemberGroupType policyType) {
        if(policyType == null){
            throw new NullPointerException("policyType can't be null");
        }
        this.groupType = policyType;
        return this;
    }

    public PartitionGroupConfig addMemberGroupConfig(MemberGroupConfig config) {
        memberGroupConfigs.add(config);
        return this;
    }

    public Collection<MemberGroupConfig> getMemberGroupConfigs() {
        return Collections.unmodifiableCollection(memberGroupConfigs);
    }

    public PartitionGroupConfig clear() {
        memberGroupConfigs.clear();
        return this;
    }

    public PartitionGroupConfig setMemberGroupConfigs(Collection<MemberGroupConfig> memberGroupConfigs) {
        this.memberGroupConfigs.clear();
        this.memberGroupConfigs.addAll(memberGroupConfigs);
        return this;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append("PartitionGroupConfig");
        sb.append("{enabled=").append(enabled);
        sb.append(", groupType=").append(groupType);
        sb.append(", memberGroupConfigs=").append(memberGroupConfigs);
        sb.append('}');
        return sb.toString();
    }
}
