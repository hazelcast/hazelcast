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
import com.hazelcast.jet.function.DistributedBiFunction;

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
     * Convenience for {@link #kafka(Properties, DistributedBiFunction,
     * String...)} wrapping the output in {@code Map.Entry}.
     */
    public static <K, V> Source<Entry<K, V>> kafka(@Nonnull Properties properties, @Nonnull String... topics) {
        return Sources.fromProcessor("streamKafka", KafkaProcessors.streamKafkaP(properties, topics));
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
     * do not cause the processor to fail.
     * Kafka consumer also does not return from {@code poll(timeout)} if the
     * cluster is down. If {@link
     * com.hazelcast.jet.config.JobConfig#setSnapshotIntervalMillis(long)
     * snapshotting is enabled}, entire job might be blocked. This is a known
     * issue of Kafka (KAFKA-1894).
     * Refer to Kafka documentation for details.
     *
     * <h4>Issue when "catching up"</h4>
     * The processor reads partitions one by one: it gets events from one
     * partition and then moves to the next one etc. This adds time disorder to
     * events: it might emit very recent event from partition1 while not yet
     * emitting an old event from partition2. If watermarks are added to the
     * stream later, the allowed event lag should accommodate this disorder.
     * Most notably, the "catching up" happens after the job is restarted, when
     * events since the last snapshot are reprocessed in a burst. In order to
     * not lose any events, the lag should be configured to at least {@code
     * snapshotInterval + timeToRestart + normalEventLag}.
     * We plan to address this issue in a future release.
     *
     * @param properties consumer properties broker address and key/value deserializers
     * @param projectionFn function to create output objects from key and value
     * @param topics     the list of topics
     */
    public static <K, V, T> Source<T> kafka(
            @Nonnull Properties properties,
            @Nonnull DistributedBiFunction<K, V, T> projectionFn,
            @Nonnull String... topics
    ) {
        return Sources.fromProcessor("streamKafka", KafkaProcessors.streamKafkaP(properties, projectionFn, topics));
    }
}
