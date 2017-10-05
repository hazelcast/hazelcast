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
import com.hazelcast.jet.function.DistributedFunction;

import java.util.Map.Entry;
import java.util.Properties;

/**
 * Contains factory methods for Apache Kafka sinks.
 */
public final class KafkaSinks {

    private KafkaSinks() {
    }

    /**
     * Returns a source that publishes messages to an Apache Kafka topic.
     * It transforms each received item to a key-value pair using the two
     * supplied mapping functions.
     * <p>
     * The source creates a single {@code KafkaProducer} per cluster
     * member using the supplied properties.
     * <p>
     * Behavior on job restart: the processor is stateless. If the job is
     * restarted, duplicate events can occur. If you need exactly once
     * behavior, you must ensure idempotence on the application level.
     *
     * @param topic          name of the Kafka topic to publish to
     * @param properties     producer properties which should contain broker
     *                       address and key/value serializers
     * @param extractKeyFn   function that extracts the key from the stream item
     * @param extractValueFn function that extracts the value from the stream item
     *
     * @param <E> type of stream item
     * @param <K> type of the key published to Kafka
     * @param <V> type of the value published to Kafka
     */
    public static <E, K, V> Sink<E> writeKafka(
            String topic, Properties properties,
            DistributedFunction<? super E, K> extractKeyFn,
            DistributedFunction<? super E, V> extractValueFn
    ) {
        return Sinks.fromProcessor("writeKafka",
                KafkaProcessors.writeKafkaP(topic, properties, extractKeyFn, extractValueFn));
    }

    /**
     * Convenience for {@link #writeKafka(String, Properties,
     * DistributedFunction, DistributedFunction)} which expects {@code
     * Map.Entry<K, V>} as input and extracts its key and value parts to be
     * published to Kafka.
     *
     * @param topic      Kafka topic name to publish to
     * @param properties producer properties which should contain broker
     *                   address and key/value serializers
     *
     * @param <K> type of the key published to Kafka
     * @param <V> type of the value published to Kafka
     */
    public static <K, V> Sink<Entry<K, V>> writeKafka(String topic, Properties properties) {
        return writeKafka(topic, properties, Entry::getKey, Entry::getValue);
    }
}
