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

package com.hazelcast.partition.client;

import com.hazelcast.instance.Node;
import com.hazelcast.nio.Protocol;
import com.hazelcast.partition.Partition;
import com.hazelcast.partition.PartitionServiceImpl;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class PartitionsHandler extends PartitionCommandHandler {

    public PartitionsHandler(PartitionServiceImpl partitionService) {
        super(partitionService);
    }

    @Override
    public Protocol processCall(Node node, Protocol protocol) {
        List<String> args = new ArrayList<String>();
        if (protocol.buffers.length > 0) {
            Partition partition = partitionService.getPartitionServiceProxy().getPartition(protocol.buffers[0]);
            args.add(String.valueOf(partition.getPartitionId()));
            args.add(partition.getOwner().getInetSocketAddress().toString());
        } else {
            Set<Partition> set = partitionService.getPartitionServiceProxy().getPartitions();
            for (Partition partition : set) {
                args.add(String.valueOf(partition.getPartitionId()));
                args.add(partition.getOwner().getInetSocketAddress().toString());
            }
        }
        return protocol.success(args.toArray(new String[0]));
    }
}
