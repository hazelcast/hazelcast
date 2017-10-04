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

package com.hazelcast.jet.core.processor;

import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.function.DistributedFunction;
import com.hazelcast.jet.impl.connector.kafka.StreamKafkaP;
import com.hazelcast.jet.impl.connector.kafka.WriteKafkaP;
import com.hazelcast.util.Preconditions;

import java.util.Arrays;
import java.util.Properties;

/**
 * Static utility class with factories of Apache Kafka source and sink
 * processors.
 */
public final class KafkaProcessors {

    private KafkaProcessors() {
    }

    /**
     * Returns a supplier of processors for a vertex that consumes one or
     * more Apache Kafka topics and emits items from them as {@code Map.Entry}
     * instances.
     * <p>
     * The meta-supplier creates a {@code KafkaConsumer} for each {@code
     * Processor} instance using the supplied {@code properties}. It assigns a
     * subset of Kafka partitions to each of them using manual partition
     * assignment (the {@code group.id} property is ignored).
     * <p>
     * If snapshotting is enabled, partition offsets are saved to the snapshot.
     * After restart, the events are emitted from the same offset.
     * <p>
     * If snapshotting is disabled, offsets are committed to kafka using
     * {@link org.apache.kafka.clients.consumer.KafkaConsumer#commitSync()
     * commitSync()}. Note however, that offsets can be committed before or
     * after the event is fully processed.
     * <p>
     * At the start of the job, partition offsets are reset depending on the key
     * {@code auto.offset.reset} property. Added partitions that are detected
     * later are always consumed from the earliest offset.
     * <p>
     * The processor completes only in case of an error or if the job is
     * cancelled.
     *
     * @param properties consumer properties which should contain consumer
     *                   group name, broker address and key/value deserializers
     * @param topics     the list of topics
     */
    public static ProcessorMetaSupplier streamKafka(Properties properties, String... topics) {
        Preconditions.checkPositive(topics.length, "At least one topic must be supplied");
        properties.put("enable.auto.commit", false);

        return new StreamKafkaP.MetaSupplier(properties, Arrays.asList(topics));
    }

    /**
     * Returns a meta-supplier of processors for a vertex that publishes
     * messages to an Apache Kafka topic. It transforms each received item
     * to a key-value pair using the two supplied mapping functions.
     * <p>
     * The meta-supplier creates a single {@code KafkaProducer} per cluster
     * member using the supplied properties. All {@code Processor}
     * instances on that member share the same producer.
     * <p>
     * Behavior on job restart: the processor is stateless. If the job is
     * restarted, duplicate events can occur. If you need exactly once
     * behaviour, idempotence must be ensured on the application level.
     *
     * @param topic          name of the Kafka topic to publish to
     * @param properties     producer properties which should contain broker
     *                       address and key/value serializers
     * @param extractKeyFn   function that extracts the key from the stream item
     * @param extractValueFn function that extracts the value from the stream item
     *
     * @param <T> type of stream item
     * @param <K> type of the key published to Kafka
     * @param <V> type of the value published to Kafka
     *
     */
    public static <T, K, V> ProcessorMetaSupplier writeKafka(
            String topic, Properties properties,
            DistributedFunction<? super T, K> extractKeyFn,
            DistributedFunction<? super T, V> extractValueFn
    ) {
        return ProcessorMetaSupplier.of(new WriteKafkaP.Supplier<>(topic, properties, extractKeyFn, extractValueFn));
    }
}
