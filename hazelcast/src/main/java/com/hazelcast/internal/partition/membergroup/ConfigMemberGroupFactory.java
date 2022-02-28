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

package com.hazelcast.internal.partition.membergroup;

import com.hazelcast.config.MemberGroupConfig;
import com.hazelcast.cluster.Member;
import com.hazelcast.cluster.impl.MemberImpl;
import com.hazelcast.internal.util.AddressUtil;
import com.hazelcast.spi.partitiongroup.MemberGroup;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import static com.hazelcast.internal.util.MapUtil.createLinkedHashMap;

public class ConfigMemberGroupFactory extends BackupSafeMemberGroupFactory implements MemberGroupFactory {

    private final Map<Integer, MemberGroupConfig> memberGroupConfigMap;

    public ConfigMemberGroupFactory(Collection<MemberGroupConfig> memberGroupConfigs) {
        this.memberGroupConfigMap = createLinkedHashMap(memberGroupConfigs.size());
        int key = 0;
        for (MemberGroupConfig groupConfig : memberGroupConfigs) {
            memberGroupConfigMap.put(key++, groupConfig);
        }
    }

    @Override
    protected Set<MemberGroup> createInternalMemberGroups(Collection<? extends Member> members) {
        Map<Integer, MemberGroup> memberGroups = new HashMap<Integer, MemberGroup>();
        for (Member member : members) {
            String host = ((MemberImpl) member).getAddress().getHost();
            for (Entry<Integer, MemberGroupConfig> entry : memberGroupConfigMap.entrySet()) {
                Collection<String> interfaces = entry.getValue().getInterfaces();
                boolean match;
                if (AddressUtil.isIpAddress(host)) {
                    match = AddressUtil.matchAnyInterface(host, interfaces);
                } else {
                    match = AddressUtil.matchAnyDomain(host, interfaces);
                }
                if (match) {
                    MemberGroup group = memberGroups.get(entry.getKey());
                    if (group == null) {
                        group = new DefaultMemberGroup();
                        memberGroups.put(entry.getKey(), group);
                    }
                    group.addMember(member);
                    break;
                }
            }
        }
        return new HashSet<MemberGroup>(memberGroups.values());
    }
}
