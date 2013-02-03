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

package com.hazelcast.impl.partition;

import com.hazelcast.cluster.AbstractRemotelyProcessable;
import com.hazelcast.cluster.MemberInfo;
import com.hazelcast.impl.PartitionManager;
import com.hazelcast.nio.Connection;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collection;

public class PartitionStateProcessable extends AbstractRemotelyProcessable {
    private PartitionRuntimeState partitionState;

    public PartitionStateProcessable(final Collection<MemberInfo> memberInfos,
                                     final PartitionInfo[] partitions,
                                     final long masterTime, int version) {
        partitionState = new PartitionRuntimeState(memberInfos, partitions, masterTime, version);
    }

    public PartitionStateProcessable() {
    }

    public void process() {
        PartitionManager partitionManager = node.concurrentMapManager.getPartitionManager();
        partitionManager.setPartitionRuntimeState(partitionState);
    }

    public void readData(DataInput in) throws IOException {
        partitionState = new PartitionRuntimeState();
        partitionState.readData(in);
    }

    public void writeData(DataOutput out) throws IOException {
        partitionState.writeData(out);
    }

    @Override
    public void setConnection(final Connection conn) {
        super.setConnection(conn);
        partitionState.setConnection(conn);
    }
}
