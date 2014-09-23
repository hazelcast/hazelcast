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

package com.hazelcast.monitor;

import com.hazelcast.management.JsonSerializable;
import com.hazelcast.management.SerializableClientEndPoint;
import com.hazelcast.management.SerializableMXBeans;
import java.util.Collection;
import java.util.List;
import java.util.Map;

public interface MemberState extends JsonSerializable {

    String getAddress();

    Map<String, Long> getRuntimeProps();

    LocalMapStats getLocalMapStats(String mapName);

    LocalMultiMapStats getLocalMultiMapStats(String mapName);

    LocalQueueStats getLocalQueueStats(String queueName);

    LocalTopicStats getLocalTopicStats(String topicName);

    LocalExecutorStats getLocalExecutorStats(String executorName);

    LocalCacheStats getLocalCacheStats(String cacheName);

    List<Integer> getPartitions();

    Collection<SerializableClientEndPoint> getClients();

    SerializableMXBeans getMXBeans();

}