/*
 * Copyright (c) 2008-2012, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.spi.AbstractOperation;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class PartitionStateOperation extends AbstractOperation implements Runnable {

    private PartitionRuntimeState partitionState;

    public PartitionStateOperation(final Collection<MemberImpl> members,
                                   final PartitionInfo[] partitions,
                                   final Collection<MigrationInfo> migrationInfos,
                                   final long masterTime, int version) {
        final List<MemberInfo> memberInfos = new ArrayList<MemberInfo>(members.size());
        for (MemberImpl member : members) {
            memberInfos.add(new MemberInfo(member.getAddress(), member.getNodeType(), member.getUuid()));
        }
        partitionState = new PartitionRuntimeState(memberInfos, partitions, migrationInfos, masterTime, version);
    }

    public PartitionStateOperation() {
    }

    public void run() {
        partitionState.setEndpoint(getCaller());
        PartitionService partitionService = getService();
        partitionService.processPartitionRuntimeState(partitionState);
    }

    public void readInternal(DataInput in) throws IOException {
        partitionState = new PartitionRuntimeState();
        partitionState.readData(in);
    }

    public void writeInternal(DataOutput out) throws IOException {
        partitionState.writeData(out);
    }
}
