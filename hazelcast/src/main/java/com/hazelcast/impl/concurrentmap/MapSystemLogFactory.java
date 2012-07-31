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
import com.hazelcast.impl.Constants.RedoType;
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

    public static SystemLog newScheduleRequest(Request request, Record record) {
        int scheduledActionCount = (record == null) ? 0 : record.getScheduledActionCount();
        DistributedLock lock = (record == null) ? null : record.getLock();
        return new RequestScheduled(request.name, request.operation, request.caller, lock, scheduledActionCount);
    }

    public static SystemLog newRedoLog(Node node, Request request, RedoType redoType, boolean isCaller) {
        final Set<Member> members = new HashSet<Member>(node.getClusterImpl().getMembers());
        final Data key = request.key;
        PartitionInfo partitionInfo = null;
        PartitionManager pm = node.partitionManager;
        if (key != null) {
            partitionInfo = new PartitionInfo(pm.getPartition(pm.getPartitionId(key)));
        }
        final Address endpoint = isCaller ? request.target : request.caller;
        boolean connected = false;
        if (endpoint != null && !endpoint.equals(node.getThisAddress())) {
            Connection targetConnection = node.connectionManager.getConnection(endpoint);
            connected = (targetConnection != null && targetConnection.live());
        }
        return new RedoLog(request.name, key, request.operation, endpoint, connected,
                members, partitionInfo, request.redoCount, pm.getMigratingPartition(), redoType, isCaller);
    }

    static class RedoLog extends SystemLog {

        final String name;
        final Data key;
        final ClusterOperation operation;
        final Address endpoint;
        final boolean connected;
        final Set<Member> members;
        final PartitionInfo partition;
        final MigratingPartition migratingPartition;
        final int redoCount;
        final RedoType redoType;
        final boolean caller;

        RedoLog(final String name, Data key, ClusterOperation operation,
                Address endpoint,
                boolean connected,
                Set<Member> members,
                PartitionInfo partition,
                int redoCount, MigratingPartition migratingPartition,
                final RedoType redoType, final boolean caller) {
            this.name = name;
            this.key = key;
            this.operation = operation;
            this.endpoint = endpoint;
            this.connected = connected;
            this.members = members;
            this.partition = partition;
            this.redoCount = redoCount;
            this.migratingPartition = migratingPartition;
            this.redoType = redoType;
            this.caller = caller;
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
            sb.append("name=").append(name).append(", ");
            sb.append("redoType=").append(redoType).append(", ");
//            sb.append("keySize=").append(key != null ? key.size() : 0);
            sb.append("operation=").append(operation)
                    .append(caller ? ", target=" : ", caller=").append(endpoint)
                    .append(" / connected=").append(connected)
                    .append(", redoCount=").append(redoCount)
                    .append(", migrating=").append(migratingPartition).append("\n")
                    .append("partition=").append(partition).append("\n");
            if (partition != null) {
                for (int i = 0; i < PartitionInfo.MAX_REPLICA_COUNT; i++) {
                    Address replicaAddress = partition.getReplicaAddress(i);
                    if (replicaAddress != null && !contains(replicaAddress)) {
                        sb.append(replicaAddress).append(" not a member!\n");
                    }
                }
            }
            sb.append("}");
            return sb.toString();
        }
    }

    static class RequestScheduled extends SystemLog {
        private final String name;
        private final ClusterOperation operation;
        private final Address caller;
        private final DistributedLock lock;
        private final int size;

        public RequestScheduled(final String name, final ClusterOperation operation,
                                final Address caller, DistributedLock lock, int size) {
            this.name = name;
            this.operation = operation;
            this.caller = caller;
            this.lock = lock;
            this.size = size;
        }

        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder();
            sb.append("RequestScheduled[").append(size).append(']');
            sb.append(" {name='").append(name).append('\'');
            sb.append(", caller=").append(caller);
            sb.append(", operation=").append(operation);
            sb.append(", lock=").append(lock);
            sb.append('}');
            return sb.toString();
        }
    }
}
