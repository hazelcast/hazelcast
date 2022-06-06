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
 * by the {@linkplain KafkaProcessors#streamKafkaP Kafka processor}.
 */
public class TopicsConfig implements Serializable {

    private final Map<String, TopicConfig> topicConfigs = new HashMap<>();

    /**
     * Returns the map of {@linkplain TopicConfig topic configurations},
     * mapped by topic name.
     */
    public Map<String, TopicConfig> getTopicConfigs() {
        return unmodifiableMap(topicConfigs);
    }

    /**
     * Returns the set of topic names from {@linkplain TopicConfig topic
     * configurations}.
     */
    public Set<String> getTopicNames() {
        return topicConfigs.keySet();
    }

    /**
     * Returns the {@linkplain TopicConfig topic configuration} for given
     * topic.
     */
    @Nullable
    public TopicConfig getTopicConfig(String topicName) {
        return topicConfigs.get(topicName);
    }

    /**
     * Adds the {@linkplain TopicConfig topic configuration}. The configuration
     * is saved under the topic name.
     */
    public TopicsConfig addTopicConfig(TopicConfig config) {
        topicConfigs.put(config.topicName, config);
        return this;
    }

    /**
     * Creates empty {@linkplain TopicConfig topic configuration} and saves it
     * in the map under the topic name.
     */
    public TopicsConfig addTopic(String topicName) {
        addTopicConfig(new TopicConfig(topicName));
        return this;
    }

    /**
     * Creates new {@linkplain TopicConfig topic configurations} for every
     * provided topic from the list and saves them in the map.
     */
    public TopicsConfig addTopics(List<String> topicNames) {
        for (String topicName : topicNames) {
            addTopicConfig(new TopicConfig(topicName));
        }
        return this;
    }

    /**
     * Returns initial offset value for given topic and partition combination.
     * If configuration for specified topic does not exist, or if initial offset is
     * not defined for given partition then {@code null} value is returned.
     */
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

        /**
         * Returns the name of the topic.
         */
        public String getTopicName() {
            return topicName;
        }

        /**
         * Returns partitions initial offsets map.
         */
        public Map<Integer, Long> getPartitionsInitialOffsets() {
            return unmodifiableMap(partitionsInitialOffsets);
        }

        /**
         * Returns the initial offset for given partition or {@code null}
         * if it was not defined.
         */
        @Nullable
        public Long getPartitionInitialOffset(int partition) {
            return partitionsInitialOffsets.get(partition);
        }

        /**
         * Adds the initial offset for given partition to the configuration.
         *
         * @param partition the number of partition
         * @param offset the initial offset for the partition
         */
        public TopicConfig addPartitionInitialOffset(int partition, long offset) {
            partitionsInitialOffsets.put(partition, offset);
            return this;
        }
    }
}
