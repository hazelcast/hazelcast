/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.kafka;

import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.function.DistributedFunction;
import com.hazelcast.jet.pipeline.StreamSource;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import javax.annotation.Nonnull;
import java.util.Map.Entry;
import java.util.Properties;

import static com.hazelcast.jet.Util.entry;
import static com.hazelcast.jet.kafka.KafkaProcessors.streamKafkaP;
import static com.hazelcast.jet.pipeline.Sources.streamFromProcessorWithWatermarks;
import static com.hazelcast.util.Preconditions.checkPositive;

/**
 * Contains factory methods for Apache Kafka sources.
 */
public final class KafkaSources {

    private KafkaSources() {
    }

    /**
     * Convenience for {@link #kafka(Properties, DistributedFunction, String...)}
     * wrapping the output in {@code Map.Entry}.
     */
    @Nonnull
    public static <K, V> StreamSource<Entry<K, V>> kafka(
            @Nonnull Properties properties,
            @Nonnull String ... topics
    ) {
        return KafkaSources.<K, V, Entry<K, V>>kafka(properties, r -> entry(r.key(), r.value()), topics);
    }

    /**
     * Returns a source that consumes one or more Apache Kafka topics and emits
     * items from them as {@code Map.Entry} instances.
     * <p>
     * The source creates a {@code KafkaConsumer} for each {@link Processor}
     * instance using the supplied {@code properties}. It assigns a subset of
     * Kafka partitions to each of them using manual partition assignment (it
     * ignores the {@code group.id} property). Default local parallelism for
     * this processor is 2 (or less if less CPUs are available).
     * <p>
     * If snapshotting is enabled, partition offsets are saved to the snapshot.
     * After restart, the source emits the events from the same offset.
     * <p>
     * If and only if snapshotting is disabled, the source commits the offsets
     * to Kafka using {@link KafkaConsumer#commitSync()}. Note however that
     * offsets can be committed before or after the event is fully processed.
     * You can configure {@code group.id} in this case.
     * <p>
     * If you add Kafka partitions at run-time, consumption from them will
     * start after a delay, based on the {@code metadata.max.age.ms} Kafka
     * property. Note, however, that events from them can be dropped as late if
     * the allowed lag is not large enough.
     * <p>
     * The processor completes only in the case of an error or if the job is
     * cancelled. IO failures are generally handled by Kafka producer and
     * do not cause the processor to fail.
     * Kafka consumer also does not return from {@code poll(timeout)} if the
     * cluster is down. If {@link JobConfig#setSnapshotIntervalMillis(long)
     * snapshotting is enabled}, entire job might be blocked. This is a known
     * issue of Kafka (KAFKA-1894).
     * Refer to Kafka documentation for details.
     * <p>
     * Default local parallelism for this processor is 4 (or less if less CPUs
     * are available). Note that deserialization is done inside {@code
     * KafkaConsumer}. If you have high traffic, the deserialization might
     * become a bottleneck - increase the local parallelism or use {@code
     * byte[]} for messages and deserialize manually in a subsequent mapping
     * step.
     *
     * @param properties consumer properties broker address and key/value
     *                  deserializers
     * @param projectionFn function to create output objects from the Kafka record.
     *                    If the projection returns a {@code null} for an item,
     *                    that item will be filtered out.
     * @param topics the topics to consume, at least one is required
     */
    @Nonnull
    public static <K, V, T> StreamSource<T> kafka(
            @Nonnull Properties properties,
            @Nonnull DistributedFunction<ConsumerRecord<K, V>, T> projectionFn,
            @Nonnull String ... topics
    ) {
        checkPositive(topics.length, "At least one topic required");
        return streamFromProcessorWithWatermarks("streamKafka",
                w -> streamKafkaP(properties, projectionFn, w, topics));
    }
}
