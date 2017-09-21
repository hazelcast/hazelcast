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

package com.hazelcast.jet.processor;

import com.hazelcast.jet.ProcessorMetaSupplier;
import com.hazelcast.jet.impl.connector.kafka.StreamKafkaP;
import com.hazelcast.jet.impl.connector.kafka.WriteKafkaP;
import com.hazelcast.util.Preconditions;

import java.util.Arrays;
import java.util.Properties;

/**
 * Static utility class with factories of Apache Kafka source and sink processors.
 */
public final class KafkaProcessors {

    private KafkaProcessors() {
    }

    /**
     * Returns a supplier of processor that consumes one or more Apache Kafka
     * topics and emits items from them as {@code Map.Entry} instances.
     * <p>
     * One {@code KafkaConsumer} is created per {@code Processor} instance
     * using the supplied {@code properties}. Subset of Kafka partitions is
     * assigned to each of them using manual partition assignment (the {@code
     * group.id} property is ignored).
     * <p>
     * If snapshotting is enabled, partition offsets are saved to the snapshot.
     * After restart, the events are emitted from the same offset.
     * <p>
     * If snapshotting is disabled, offsets are committed to kafka using
     * {@link org.apache.kafka.clients.consumer.KafkaConsumer#commitSync()
     * commitSync()}. Note however, that offsets can be committed before or
     * after the event is fully processed.
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
     * Returns a meta-supplier of processor that publishes messages to an
     * Apache Kafka topic. It expects items of type {@code Map.Entry<K,V>} on
     * input and publishes them to Apache Kafka.
     * <p>
     * A single {@code KafkaProducer} is created per node using the supplied
     * properties file. The producer instance is shared across all {@code
     * Processor} instances on that node.
     * <p>
     * Behavior on job restart: the processor is stateless. If the job is
     * restarted, duplicate events can occur. If you need exactly once
     * behaviour, idempotence must be ensured on the application level.
     *
     * @param <K>        type of keys written
     * @param <V>        type of values written
     * @param topic      Kafka topic name to publish to
     * @param properties producer properties which should contain broker
     *                   address and key/value serializers
     */
    public static <K, V> ProcessorMetaSupplier writeKafka(String topic, Properties properties) {
        return ProcessorMetaSupplier.of(new WriteKafkaP.Supplier<K, V>(topic, properties));
    }
}
