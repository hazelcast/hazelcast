/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet;

import com.hazelcast.jet.core.processor.KafkaProcessors;

import javax.annotation.Nonnull;
import java.util.Map.Entry;
import java.util.Properties;

/**
 * Contains factory methods for Apache Kafka sources.
 */
public final class KafkaSources {

    private KafkaSources() {
    }

    /**
     * Returns a source that consumes one or more Apache Kafka topics and emits
     * items from them as {@code Map.Entry} instances.
     * <p>
     * The source creates a {@code KafkaConsumer} for each {@link
     * com.hazelcast.jet.core.Processor processor} instance using the supplied
     * {@code properties}. It assigns a subset of Kafka partitions to each of
     * them using manual partition assignment (it ignores the {@code group.id}
     * property).
     * <p>
     * If snapshotting is enabled, partition offsets are saved to the snapshot.
     * After restart, the source emits the events from the same offset.
     * <p>
     * If snapshotting is disabled, the source commits the offsets to Kafka
     * using {@link org.apache.kafka.clients.consumer.KafkaConsumer#commitSync()
     * commitSync()}. Note however that offsets can be committed before the
     * event is fully processed.
     * <p>
     * The processor completes only in the case of an error or if the job is
     * cancelled. IO failures are generally handled by Kafka producer and
     * generally do not cause the processor to fail. Refer to Kafka
     * documentation for details.
     *
     * @param properties consumer properties broker address and key/value deserializers
     * @param topics     the list of topics
     */
    public static <K, V> Source<Entry<K, V>> streamKafka(@Nonnull Properties properties, @Nonnull String... topics) {
        return Sources.fromProcessor("streamKafka", KafkaProcessors.streamKafkaP(properties, topics));
    }
}
