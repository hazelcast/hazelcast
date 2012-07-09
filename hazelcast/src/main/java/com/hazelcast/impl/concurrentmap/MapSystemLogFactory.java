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

package com.hazelcast.impl.concurrentmap;

import com.hazelcast.core.Member;
import com.hazelcast.impl.*;
import com.hazelcast.impl.base.DistributedLock;
import com.hazelcast.impl.base.SystemLog;
import com.hazelcast.impl.partition.MigratingPartition;
import com.hazelcast.impl.partition.PartitionInfo;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.Connection;
import com.hazelcast.nio.Data;

import java.util.HashSet;
import java.util.Set;

public class MapSystemLogFactory {

    public static SystemLog newScheduleRequest(DistributedLock lock, int size) {
        return new RequestScheduled(lock, size);
    }

    public static SystemLog newRedoLog(Node node, Request request) {
        final Set<Member> members = new HashSet<Member>(node.getClusterImpl().getMembers());
        final Data key = request.key;
        final Address target = request.target;
        PartitionInfo partitionInfo = null;
        PartitionManager pm = node.concurrentMapManager.getPartitionManager();
        if (key != null) {
            partitionInfo = new PartitionInfo(pm.getPartition(node.concurrentMapManager.getPartitionId(key)));
        }
        boolean targetConnected = false;
        if (target != null && node.getThisAddress().equals(target)) {
            Connection targetConnection = node.connectionManager.getConnection(target);
            targetConnected = (targetConnection != null && targetConnection.live());
        }
        return new RedoLog(key, request.operation, target, targetConnected,
                members, partitionInfo, request.redoCount, pm.getMigratingPartition());
    }

    static class RedoLog extends SystemLog {
        final Data key;
        final ClusterOperation operation;
        final Address target;
        final boolean targetConnected;
        final Set<Member> members;
        final PartitionInfo partition;
        final MigratingPartition migratingPartition;
        final int redoCount;

        RedoLog(Data key, ClusterOperation operation,
                Address target,
                boolean targetConnected,
                Set<Member> members,
                PartitionInfo partition,
                int redoCount, MigratingPartition migratingPartition) {
            this.key = key;
            this.operation = operation;
            this.target = target;
            this.targetConnected = targetConnected;
            this.members = members;
            this.partition = partition;
            this.redoCount = redoCount;
            this.migratingPartition = migratingPartition;
        }

        private boolean contains(Address address) {
            for (Member member : members) {
                MemberImpl m = (MemberImpl) member;
                if (m.getAddress().equals(address)) {
                    return true;
                }
            }
            return false;
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder("RedoLog{");
            sb.append("key=" + key +
                    ", operation=" + operation +
                    ", target=" + target +
                    ", targetConnected=" + targetConnected +
                    ", redoCount=" + redoCount +
                    ", migrating=" + migratingPartition + "\n" +
                    "partition=" + partition +
                    "\n");
            if (partition != null) {
                for (int i = 0; i < PartitionInfo.MAX_REPLICA_COUNT; i++) {
                    Address replicaAddress = partition.getReplicaAddress(i);
                    if (replicaAddress != null && !contains(replicaAddress)) {
                        sb.append(replicaAddress + " not a member!\n");
                    }
                }
            }
            sb.append("}");
            return sb.toString();
        }
    }

    static class RequestScheduled extends SystemLog {
        private final DistributedLock lock;
        private final int size;

        public RequestScheduled(DistributedLock lock, int size) {
            this.lock = lock;
            this.size = size;
        }

        @Override
        public String toString() {
            DistributedLock l = lock;
            StringBuilder sb = new StringBuilder("Scheduled[size=");
            sb.append(size).append("]");
            if (l != null) {
                sb.append(" {");
                sb.append(l.toString());
                sb.append("}");
            }
            return sb.toString();
        }
    }
}
