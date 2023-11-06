/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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
package com.hazelcast.rest.service;

import com.hazelcast.cluster.Member;
import com.hazelcast.rest.model.MemberDetailModel;
import com.hazelcast.rest.util.NodeEngineImplHolder;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

@Service
public class MemberService {
    private final NodeEngineImplHolder nodeEngineImplHolder;

    public MemberService(NodeEngineImplHolder nodeEngineImplHolder) {
        this.nodeEngineImplHolder = nodeEngineImplHolder;
    }

    public List<MemberDetailModel> getMembers(int page, int size) {

        List<MemberDetailModel> members = new ArrayList<>();
        nodeEngineImplHolder.getNodeEngine()
                .getHazelcastInstance()
                .getCluster().getMembers().stream().map(m -> new MemberDetailModel.MemberDetailModelBuilder()
                        .address(m.getAddress().toString())
                        .liteMember(m.isLiteMember())
                        .localMember(m.localMember())
                        .uuid(m.getUuid().toString())
                        .build())
                .forEach(members::add);

        int startIndex = page * size;
        int endIndex = Math.min(startIndex + size, members.size());
        if (startIndex < endIndex) {
            return members.subList(startIndex, endIndex);
        } else {
            return Collections.emptyList();
        }
    }

    public MemberDetailModel getCurrentMember() {
        Member localMember = nodeEngineImplHolder.getNodeEngine()
                .getHazelcastInstance()
                .getCluster().getLocalMember();
        return new MemberDetailModel.MemberDetailModelBuilder()
                .address(localMember.getAddress().toString())
                .liteMember(localMember.isLiteMember())
                .localMember(localMember.localMember())
                .uuid(localMember.getUuid().toString())
                .build();
    }

    public MemberDetailModel getMemberWithUuid(UUID uuid) {
        List<Member> members = nodeEngineImplHolder.getNodeEngine()
                .getHazelcastInstance()
                .getCluster().getMembers().stream().filter(i -> i.getUuid().equals(uuid)).collect(Collectors.toList());

        if (!members.isEmpty()) {
            Member member = members.get(0);
            return new MemberDetailModel.MemberDetailModelBuilder()
                    .address(member.getAddress().toString())
                    .liteMember(member.isLiteMember())
                    .localMember(member.localMember())
                    .uuid(member.getUuid().toString())
                    .build();
        } else {
            return null;
        }
    }
}
