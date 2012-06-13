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

import com.hazelcast.nio.Address;
import com.hazelcast.nio.DataSerializable;

import java.util.List;
import java.util.Map;

public interface MemberState extends DataSerializable {

    Address getAddress();

    MemberHealthStats getMemberHealthStats();

    Map<String,Long> getRuntimeProps();

    LocalMapStats getLocalMapStats(String mapName);

    LocalExecutorOperationStats getInternalExecutorStats(String name);

    LocalExecutorOperationStats getExternalExecutorStats(String name);

    LocalQueueStats getLocalQueueStats(String queueName);

    LocalTopicStats getLocalTopicStats(String topicName);

    List<Integer> getPartitions();

    LocalAtomicNumberStats getLocalAtomicNumberStats(String queueName);

    LocalCountDownLatchStats getLocalCountDownLatchStats(String queueName);

    LocalSemaphoreStats getLocalSemaphoreStats(String queueName);
}
