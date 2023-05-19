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

package com.hazelcast.internal.tpc.apps;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.partition.Partition;
import com.hazelcast.partition.PartitionService;

import java.net.SocketAddress;

@SuppressWarnings("checkstyle:MagicNumber")
public final class MainUtil {

    private MainUtil() {
    }

    public static int findPartition(HazelcastInstance node) {
        Integer x = 0;
        SocketAddress localAddress = node.getLocalEndpoint().getSocketAddress();
        PartitionService partitionService = node.getPartitionService();
        for (; ; ) {
            Partition partition = partitionService.getPartition(x);
            if (partition == null || partition.getOwner() == null) {
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                continue;
            }
            if (localAddress.equals(partition.getOwner().getSocketAddress())) {
                return partition.getPartitionId();
            }
            x++;
        }
    }
}
