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

package com.hazelcast.monitor;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.partition.Partition;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Callable;

public class DistributedMemberInfoCallable implements Callable<DistributedMemberInfoCallable.MemberInfo>, Serializable, HazelcastInstanceAware {
    private transient HazelcastInstance hazelcastInstance;

    public MemberInfo call() throws Exception {
        MemberInfo memberInfo = new MemberInfo();
        Set<Integer> partitions = new HashSet<Integer>();
        for (Partition partition : hazelcastInstance.getPartitionService().getPartitions()) {
            if (hazelcastInstance.getCluster().getLocalMember().equals(partition.getOwner())) {
                partitions.add(partition.getPartitionId());
            }
        }
        memberInfo.partitions = partitions;
        memberInfo.time = System.currentTimeMillis();
        memberInfo.totalMemory = Runtime.getRuntime().totalMemory();
        memberInfo.freeMemory = Runtime.getRuntime().freeMemory();
        memberInfo.maxMemory = Runtime.getRuntime().maxMemory();
        memberInfo.availableProcessors = Runtime.getRuntime().availableProcessors();
        memberInfo.systemProps = System.getProperties();
        return memberInfo;
    }

    public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
        this.hazelcastInstance = hazelcastInstance;
    }

    public static class MemberInfo implements Serializable {
        private Set<Integer> partitions;
        private long time;
        private long totalMemory;
        private long freeMemory;
        public long maxMemory;
        public int availableProcessors;
        public Properties systemProps;

        public Set<Integer> getPartitions() {
            return partitions;
        }

        public long getTime() {
            return time;
        }

        public long getTotalMemory() {
            return totalMemory;
        }

        public long getFreeMemory() {
            return freeMemory;
        }

        public long getMaxMemory() {
            return maxMemory;
        }

        public int getAvailableProcessors() {
            return availableProcessors;
        }

        public Properties getSystemProps() {
            return systemProps;
        }
    }
}
