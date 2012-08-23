/*
 * Copyright (c) 2008-2012, Hazel Bilisim Ltd. All Rights Reserved.
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

import com.hazelcast.cluster.MemberInfo;
import com.hazelcast.impl.Node;
import com.hazelcast.impl.spi.AbstractOperation;
import com.hazelcast.impl.spi.NoReply;
import com.hazelcast.impl.spi.NonBlockingOperation;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collection;

public class PartitionStateOperation extends AbstractOperation implements NonBlockingOperation, NoReply {
    private PartitionRuntimeState partitionState;

    public PartitionStateOperation(final Collection<MemberInfo> memberInfos,
                                   final PartitionInfo[] partitions,
                                   final long masterTime, int version) {
        partitionState = new PartitionRuntimeState(memberInfos, partitions, masterTime, version);
    }

    public PartitionStateOperation() {
    }

    public void run() {
        Node node = getNodeService().getNode();
        partitionState.setEndpoint(getCaller());
        PartitionManager partitionManager = node.partitionManager;
        partitionManager.processPartitionRuntimeState(partitionState);
    }

    public void readInternal(DataInput in) throws IOException {
        partitionState = new PartitionRuntimeState();
        partitionState.readData(in);
    }

    public void writeInternal(DataOutput out) throws IOException {
        partitionState.writeData(out);
    }
//    @Override
//    public void setConnection(final Connection conn) {
//        super.setConnection(conn);
//        partitionState.setConnection(conn);
//    }
}
