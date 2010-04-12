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

package com.hazelcast.monitor.server.event;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.monitor.client.event.ChangeEventType;

public class ChangeEventGeneratorFactory {

    public ChangeEventGenerator createEventGenerator(ChangeEventType eventType, int clusterId, String name, HazelcastClient client) {
        ChangeEventGenerator eventGenerator = null;
        if (eventType.equals(ChangeEventType.MAP_STATISTICS)) {
            eventGenerator = new MapStatisticsGenerator(client, name, clusterId);
        } else if (eventType.equals(ChangeEventType.PARTITIONS)) {
            eventGenerator = new PartitionsEventGenerator(client, clusterId);
        } else if (eventType.equals(ChangeEventType.MEMBER_INFO)) {
            eventGenerator = new MemberInfoEventGenerator(client, clusterId, name);
        } else if (eventType.equals(ChangeEventType.QUEUE_STATISTICS)){
            eventGenerator = new QueueStatisticsGenerator(client, name, clusterId);
        }

        return eventGenerator;
    }
}
