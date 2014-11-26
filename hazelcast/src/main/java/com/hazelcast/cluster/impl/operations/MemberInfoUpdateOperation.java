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

package com.hazelcast.cluster.impl.operations;

import com.hazelcast.cluster.MemberInfo;
import com.hazelcast.cluster.impl.ClusterServiceImpl;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.Connection;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.util.Clock;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;

public class MemberInfoUpdateOperation extends AbstractClusterOperation implements JoinOperation {

    private Collection<MemberInfo> memberInfos;
    private long masterTime = Clock.currentTimeMillis();
    private boolean sendResponse;

    public MemberInfoUpdateOperation() {
        memberInfos = new ArrayList<MemberInfo>();
    }

    public MemberInfoUpdateOperation(Collection<MemberInfo> memberInfos, long masterTime, boolean sendResponse) {
        this.masterTime = masterTime;
        this.memberInfos = memberInfos;
        this.sendResponse = sendResponse;
    }

    @Override
    public void run() throws Exception {
        processMemberUpdate();
    }

    protected final void processMemberUpdate() {
        if (isValid()) {
            final ClusterServiceImpl clusterService = getService();
            clusterService.setMasterTime(masterTime);
            clusterService.updateMembers(memberInfos);
        }
    }

    protected final boolean isValid() {
        final ClusterServiceImpl clusterService = getService();
        final Connection conn = getConnection();
        final Address masterAddress = conn != null ? conn.getEndPoint() : null;
        boolean isLocal = conn == null;
        return isLocal
                || (masterAddress != null && masterAddress.equals(clusterService.getMasterAddress()));
    }

    @Override
    public final boolean returnsResponse() {
        return sendResponse;
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        masterTime = in.readLong();
        int size = in.readInt();
        memberInfos = new ArrayList<MemberInfo>(size);
        while (size-- > 0) {
            MemberInfo memberInfo = new MemberInfo();
            memberInfo.readData(in);
            memberInfos.add(memberInfo);
        }
        sendResponse = in.readBoolean();
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        out.writeLong(masterTime);
        out.writeInt(memberInfos.size());
        for (MemberInfo memberInfo : memberInfos) {
            memberInfo.writeData(out);
        }
        out.writeBoolean(sendResponse);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("MemberInfoUpdateOperation {\n");
        for (MemberInfo address : memberInfos) {
            sb.append(address).append('\n');
        }
        sb.append('}');
        return sb.toString();
    }
}

