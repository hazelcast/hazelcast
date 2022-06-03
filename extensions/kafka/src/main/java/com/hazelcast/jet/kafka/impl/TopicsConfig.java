/*
 * Copyright 2021 Hazelcast Inc.
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

package com.hazelcast.jet.kafka.impl;

import com.hazelcast.jet.kafka.KafkaProcessors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.hazelcast.internal.util.Preconditions.checkNotNull;
import static java.util.Collections.unmodifiableMap;

/**
 * Contains the configuration for all Kafka topics that will be consumed
 * by {@linkplain KafkaProcessors#streamKafkaP Kafka processor}.
 */
public class TopicsConfig implements Serializable {

    private final Map<String, TopicConfig> topicConfigs = new HashMap<>();

    public Map<String, TopicConfig> getTopicConfigs() {
        return unmodifiableMap(topicConfigs);
    }

    public Set<String> getTopicNames() {
        return topicConfigs.keySet();
    }

    @Nullable
    public TopicConfig getTopicConfig(String topicName) {
        return topicConfigs.get(topicName);
    }

    public TopicsConfig putTopicConfig(TopicConfig config) {
        topicConfigs.put(config.topicName, config);
        return this;
    }

    public TopicsConfig putTopic(String topicName) {
        putTopicConfig(new TopicConfig(topicName));
        return this;
    }

    public TopicsConfig putTopics(List<String> topicNames) {
        for (String topicName : topicNames) {
            putTopicConfig(new TopicConfig(topicName));
        }
        return this;
    }

    @Nullable
    public Long getInitialOffsetFor(String topicName, int partition) {
        TopicConfig topicConfig = topicConfigs.get(topicName);
        if (topicConfig == null) {
            return null;
        }
        return topicConfig.getPartitionInitialOffset(partition);
    }

    /**
     * Contains the configuration for a single Kafka topic.
     */
    public static class TopicConfig implements Serializable {

        private final String topicName;
        private final Map<Integer, Long> partitionsInitialOffsets = new HashMap<>();

        public TopicConfig(@Nonnull String topicName) {
            checkNotNull(topicName);
            this.topicName = topicName;
        }

        @Nullable
        public Long getPartitionInitialOffset(int partition) {
            return partitionsInitialOffsets.get(partition);
        }

        public TopicConfig putPartitionInitialOffset(int partition, long offset) {
            partitionsInitialOffsets.put(partition, offset);
            return this;
        }
    }
}
