/* 
 * Copyright (c) 2008-2010, Hazel Ltd. All Rights Reserved.
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
 *
 */

package com.hazelcast.impl.partition;

import com.hazelcast.config.MemberGroupConfig;
import com.hazelcast.impl.AddressPicker;
import com.hazelcast.impl.MemberImpl;

import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;

public class ConfigMemberGroupFactory implements MemberGroupFactory {

    private final Map<Integer, MemberGroupConfig> memberGroupConfigMap;

    public ConfigMemberGroupFactory(Collection<MemberGroupConfig> memberGroupConfigs) {
        super();
        this.memberGroupConfigMap = new LinkedHashMap<Integer, MemberGroupConfig>();
        int key = 0;
        for (MemberGroupConfig groupConfig : memberGroupConfigs) {
            memberGroupConfigMap.put(key++, groupConfig);
        }
    }

    public Collection<MemberGroup> createMemberGroups(Collection<MemberImpl> members) {
        final Map<Integer, MemberGroup> memberGroups = new HashMap<Integer, MemberGroup>();
        for (MemberImpl member : members) {
            for (Entry<Integer, MemberGroupConfig> groupConfigEntry : memberGroupConfigMap.entrySet()) {
                if (AddressPicker.matchAddress(member.getAddress().getHost(), groupConfigEntry.getValue().getInterfaces())) {
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
        return memberGroups.values();
    }
}
