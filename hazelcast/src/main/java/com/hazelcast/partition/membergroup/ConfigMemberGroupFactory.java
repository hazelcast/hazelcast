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

package com.hazelcast.partition.membergroup;

import com.hazelcast.config.MemberGroupConfig;
import com.hazelcast.core.Member;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.util.AddressUtil;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

public class ConfigMemberGroupFactory extends BackupSafeMemberGroupFactory implements MemberGroupFactory {

    private final Map<Integer, MemberGroupConfig> memberGroupConfigMap;

    public ConfigMemberGroupFactory(Collection<MemberGroupConfig> memberGroupConfigs) {
        this.memberGroupConfigMap = new LinkedHashMap<Integer, MemberGroupConfig>();
        int key = 0;
        for (MemberGroupConfig groupConfig : memberGroupConfigs) {
            memberGroupConfigMap.put(key++, groupConfig);
        }
    }

    @Override
    protected Set<MemberGroup> createInternalMemberGroups(Collection<? extends Member> members) {
        Map<Integer, MemberGroup> memberGroups = new HashMap<Integer, MemberGroup>();
        for (Member member : members) {
            for (Entry<Integer, MemberGroupConfig> groupConfigEntry : memberGroupConfigMap.entrySet()) {
                if (AddressUtil.matchAnyInterface(((MemberImpl) member).getAddress().getHost(),
                        groupConfigEntry.getValue().getInterfaces())) {
                    MemberGroup group = memberGroups.get(groupConfigEntry.getKey());
                    if (group == null) {
                        group = new DefaultMemberGroup();
                        memberGroups.put(groupConfigEntry.getKey(), group);
                    }
                    group.addMember(member);
                    break;
                }
            }
        }
        return new HashSet<MemberGroup>(memberGroups.values());
    }
}
