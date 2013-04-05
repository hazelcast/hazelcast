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

package com.hazelcast.partition;

import com.hazelcast.cluster.MemberInfo;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.AbstractOperation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public final class PartitionStateOperation extends AbstractOperation implements MigrationCycleOperation {

    private PartitionRuntimeState partitionState;

    public PartitionStateOperation(final Collection<MemberImpl> members,
                                   final PartitionInfo[] partitions,
                                   final Collection<MigrationInfo> migrationInfos,
                                   final long masterTime, int version) {
        final List<MemberInfo> memberInfos = new ArrayList<MemberInfo>(members.size());
        for (MemberImpl member : members) {
            memberInfos.add(new MemberInfo(member.getAddress(), member.getUuid()));
        }
        partitionState = new PartitionRuntimeState(memberInfos, partitions, migrationInfos, masterTime, version);
    }

    public PartitionStateOperation() {
    }

    public void run() {
        partitionState.setEndpoint(getCallerAddress());
        PartitionServiceImpl partitionService = getService();
        partitionService.processPartitionRuntimeState(partitionState);
    }

    @Override
    public boolean returnsResponse() {
        return false;
    }

    @Override
    public String getServiceName() {
        return PartitionServiceImpl.SERVICE_NAME;
    }

    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        partitionState = new PartitionRuntimeState();
        partitionState.readData(in);
    }

    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        partitionState.writeData(out);
    }
}
