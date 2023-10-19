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

package com.hazelcast.internal.cluster.impl.operations;

import com.hazelcast.cluster.Address;
import com.hazelcast.cluster.Member;
import com.hazelcast.internal.cluster.impl.ClusterDataSerializerHook;
import com.hazelcast.internal.cluster.impl.ClusterServiceImpl;
import com.hazelcast.internal.cluster.impl.MembersView;
import com.hazelcast.internal.cluster.impl.MembersViewResponse;
import com.hazelcast.internal.cluster.impl.MembershipManager;
import com.hazelcast.internal.partition.operation.DemoteRequestOperation;
import com.hazelcast.internal.partition.operation.DemoteResponseOperation;

import java.util.UUID;

/**
 * Demotes caller data member to a lite member. Should be executed on only master node. This operation may only be
 * executed <i>after</i> a {@link DemoteResponseOperation} has been received.
 * <p/>
 * The overall sequence of operations to demote a member is;
 * <ol>
 *     <li>{@link DemoteRequestOperation} (initiated by demoted member, executed on master)</li>
 *     <li>{@link DemoteResponseOperation} (initiated by master, executed on demoted member)</li>
 *     <li>{@link DemoteDataMemberOp} (initiated by demoted member, executed on master)</li>
 * </ol>
 *
 * @since 5.4
 */
public class DemoteDataMemberOp extends AbstractClusterOperation {

    private transient MembersViewResponse response;

    public DemoteDataMemberOp() {
    }

    @Override
    public void run() throws Exception {
        ClusterServiceImpl service = getService();
        Address callerAddress = getCallerAddress();
        UUID callerUuid = getCallerUuid();

        MembershipManager membershipManager = service.getMembershipManager();
        MembersView membersView = membershipManager.demoteToLiteMember(callerAddress, callerUuid);
        Member localMember = getNodeEngine().getLocalMember();
        this.response = new MembersViewResponse(localMember.getAddress(), localMember.getUuid(), membersView);
    }

    @Override
    public boolean returnsResponse() {
        return true;
    }

    @Override
    public Object getResponse() {
        return response;
    }

    @Override
    public int getClassId() {
        return ClusterDataSerializerHook.DEMOTE_DATA_MEMBER;
    }
}
