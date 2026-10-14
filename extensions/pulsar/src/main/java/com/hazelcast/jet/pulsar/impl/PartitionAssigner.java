/*
 * Copyright 2026 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hazelcast.jet.pulsar.impl;

import com.hazelcast.jet.core.Processor;
import org.apache.pulsar.common.naming.TopicName;

import java.util.List;

class PartitionAssigner {
    private final List<TopicName> allTopicNames;
    private final int totalParallelism;
    private final int globalProcessorIndex;

    PartitionAssigner(List<String> userProvidedTopics, Processor.Context context) {
        allTopicNames = userProvidedTopics.stream().map(TopicName::get).toList();
        totalParallelism = context.totalParallelism();
        globalProcessorIndex = context.globalProcessorIndex();
    }

    boolean belongsToThisProcessor(TopicName topicName) {
        TopicName withoutPartition = TopicName.get(topicName.getPartitionedTopicName());
        int topicIndex = allTopicNames.indexOf(withoutPartition);
        int topicsCount = allTopicNames.size();

        int startIndex = topicIndex * Math.max(1, totalParallelism / topicsCount);
        int partitionIndex = topicName.getPartitionIndex() == -1 ? 0 : topicName.getPartitionIndex();
        int topicPartitionHandledBy = (startIndex + partitionIndex) % totalParallelism;
        return topicPartitionHandledBy == globalProcessorIndex;
    }

}
