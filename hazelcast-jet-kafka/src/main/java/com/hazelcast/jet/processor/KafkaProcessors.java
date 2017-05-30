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

import com.hazelcast.jet.Processor;
import com.hazelcast.jet.ProcessorMetaSupplier;
import com.hazelcast.jet.function.DistributedSupplier;
import com.hazelcast.jet.impl.connector.kafka.StreamKafkaP;
import com.hazelcast.jet.impl.connector.kafka.WriteKafkaP;
import com.hazelcast.util.Preconditions;

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
     * using the supplied {@code properties}. All processors are in the same
     * consumer group as specified by the {@code group.id} property. The
     * supplied properties will be passed on to the {@code KafkaConsumer}
     * instance.
     * <p>
     * The processor completes only in case of an error or if the job is
     * cancelled.
     *
     * @param properties consumer properties which should contain consumer
     *                   group name, broker address and key/value deserializers
     * @param topics     the list of topics
     */
    public static DistributedSupplier<Processor> streamKafka(Properties properties, String... topics) {
        Preconditions.checkPositive(topics.length, "At least one topic must be supplied");
        Preconditions.checkTrue(properties.containsKey("group.id"), "Properties should contain `group.id`");
        properties.put("enable.auto.commit", false);

        return () -> new StreamKafkaP(properties, topics);
    }

    /**
     * Returns a meta-supplier of processor that publishes messages to an
     * Apache Kafka topic. It expects items of type {@code Map.Entry<K,V>} on
     * input and publishes them to Apache Kafka.
     *
     * A single {@code KafkaProducer} is created per node using the supplied
     * properties file. The producer instance is shared across all {@code
     * Processor} instances on that node.
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
