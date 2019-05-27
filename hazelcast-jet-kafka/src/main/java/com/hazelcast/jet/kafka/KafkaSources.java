/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.jet.function.FunctionEx;
import com.hazelcast.jet.pipeline.Stage;
import com.hazelcast.jet.pipeline.StreamSource;
import com.hazelcast.jet.pipeline.StreamSourceStage;
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
 *
 * @since 3.0
 */
public final class KafkaSources {

    private KafkaSources() {
    }

    /**
     * Convenience for {@link #kafka(Properties, FunctionEx, String...)}
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
     * this processor is 2 (or less if less CPUs are available). The Kafka's
     * message timestamp will be used as a {@linkplain
     * StreamSourceStage#withNativeTimestamps(long) native timestamp}.
     * <p>
     * If snapshotting is enabled, partition offsets are saved to the snapshot.
     * After a restart, the source emits the events from the same offsets.
     * <p>
     * If you start a new job from an exported state, you can change the source
     * parameters as needed:<ul>
     *     <li>if you add a topic, it will be consumed from the default position
     *     <li>if you remove a topic, restored offsets will be ignored (there
     *     will be a warning logged)
     *     <li>if you connect to another cluster, the offsets will be used based
     *     on the equality of the topic name. To avoid this, give different
     *     {@linkplain Stage#setName name} to this source
     *     <li>if the partition count is lower after a restart, the extra
     *     offsets will be ignored
     * </ul>
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
     * The processor never completes, it can only fail in the case of an error.
     * However, IO failures are generally handled by Kafka producer and do not
     * cause the processor to fail. Kafka consumer also does not return from
     * {@code poll(timeout)} if the cluster is down. If {@link
     * JobConfig#setSnapshotIntervalMillis(long) snapshotting is enabled},
     * entire job might be blocked. This is a known issue of Kafka
     * (KAFKA-1894). Refer to Kafka documentation for details.
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
            @Nonnull FunctionEx<ConsumerRecord<K, V>, T> projectionFn,
            @Nonnull String ... topics
    ) {
        checkPositive(topics.length, "At least one topic required");
        return streamFromProcessorWithWatermarks("streamKafka(" + String.join(",", topics) + ")",
                w -> streamKafkaP(properties, projectionFn, w, topics), true);
    }
}
