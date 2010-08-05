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

package com.hazelcast.impl;

import com.hazelcast.cluster.ClusterImpl;
import com.hazelcast.core.ITopic;
import com.hazelcast.core.Member;
import com.hazelcast.core.MessageListener;
import com.hazelcast.core.MultiMap;
import com.hazelcast.monitor.ClusterViewImpl;
import com.hazelcast.partition.Partition;
import com.hazelcast.partition.PartitionService;

import java.util.ArrayList;
import java.util.Set;

public class MemberStatsPublisher implements MessageListener {
    private final MultiMap multimap;
    private final Node node;
    public final static String STATS_TOPIC_NAME = "_hz__MemberStatsTopic";
    public final static String STATS_MULTIMAP_NAME = "_hz__MemberStatsMultiMap";

    public MemberStatsPublisher(ITopic topic, MultiMap multimap, Node node) {
        this.multimap = multimap;
        this.node = node;
        topic.addMessageListener(this);
    }

    public void onMessage(final Object key) {
        node.executorManager.executeLocally(new FallThroughRunnable() {
            public void doRun() {
                if (node.joined() && node.isActive()) {
                    ClusterImpl clusterImpl = node.getClusterImpl();
                    if (node.isMaster()) {
                        ClusterViewImpl clusterView = new ClusterViewImpl(node.factory.getLongInstanceNames());
                        PartitionService partitionService = node.factory.getPartitionService();
                        Set<Member> members = clusterImpl.getMembers();
                        for (Member member : members) {
                            clusterView.setPartition(member, getPartitions(partitionService, member));
                        }
                        multimap.put(key, clusterView);
                    }
                    MemberStatsImpl memberStats = node.factory.createMemberStats();
                    multimap.put(key, memberStats);
                }
            }
        });
    }

    private int[] getPartitions(PartitionService partitionService, Member member) {
        Set<Partition> partitions = partitionService.getPartitions();
        ArrayList<Integer> ownedPartitions = new ArrayList<Integer>();
        for (Partition partition : partitions) {
            if (member.equals(partition.getOwner())) {
                ownedPartitions.add(partition.getPartitionId());
            }
        }
        int[] ownedPartitionIds = new int[ownedPartitions.size()];
        int c = 0;
        for (Integer ownedPartition : ownedPartitions) {
            ownedPartitionIds[c++] = ownedPartition;
        }
        return ownedPartitionIds;
    }
}
