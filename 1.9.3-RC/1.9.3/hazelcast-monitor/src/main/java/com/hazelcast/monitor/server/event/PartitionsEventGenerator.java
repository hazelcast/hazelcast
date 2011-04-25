/*
 * Copyright (c) 2007-2008, Hazel Ltd. All Rights Reserved.
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
package com.hazelcast.monitor.server.event;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.monitor.client.event.ChangeEvent;
import com.hazelcast.monitor.client.event.ChangeEventType;
import com.hazelcast.monitor.client.event.Partitions;

import java.util.*;

import static com.hazelcast.monitor.server.event.MembershipEventGenerator.getName;

public class PartitionsEventGenerator implements ChangeEventGenerator {
    final private HazelcastClient client;
    final private int clusterId;

    public PartitionsEventGenerator(HazelcastClient client, int clusterId) {
        this.client = client;
        this.clusterId = clusterId;
    }

    public ChangeEvent generateEvent() {
        Partitions event = new Partitions(clusterId);
        Set<com.hazelcast.partition.Partition> partitions = client.getPartitionService().getPartitions();
        Map<String, List<Integer>> map = new HashMap<String, List<Integer>>();
        for (com.hazelcast.partition.Partition partition : partitions) {
            String name = getName(partition.getOwner());
            List blocks = map.get(name);
            if (blocks == null) {
                blocks = new ArrayList<Integer>();
                map.put(name, blocks);
            }
            blocks.add(partition.getPartitionId());
        }
        for (String owner : map.keySet()) {
            List<Integer> list = map.get(owner);
            Collections.sort(list);
            StringBuilder blocks = new StringBuilder();
            for (int i : list) {
                blocks.append(i).append(", ");
            }
            String value = blocks.toString();
            event.getPartitions().put(owner, value.substring(0, value.length() - 2));
            event.getCount().put(owner, list.size());
        }
        return event;
    }

    public ChangeEventType getChangeEventType() {
        return ChangeEventType.PARTITIONS;
    }

    public int getClusterId() {
        return clusterId;
    }
}
