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

package com.hazelcast.jet;

import com.hazelcast.jet.function.DistributedFunction;
import com.hazelcast.jet.pipeline.Sink;
import com.hazelcast.jet.pipeline.Sinks;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nonnull;
import java.util.Map.Entry;
import java.util.Properties;

import static com.hazelcast.jet.core.processor.KafkaProcessors.writeKafkaP;

/**
 * Contains factory methods for Apache Kafka sinks.
 */
public final class KafkaSinks {

    private KafkaSinks() {
    }

    /**
     * Returns a source that publishes messages to an Apache Kafka topic.
     * It transforms each received item to a {@code ProducerRecord} using the
     * supplied mapping function.
     * <p>
     * The source creates a single {@code KafkaProducer} per cluster
     * member using the supplied {@code properties}.
     * <p>
     * Behavior on job restart: the processor is stateless. On snapshot we only
     * make sure that all async operations are done. If the job is restarted,
     * duplicate events can occur. If you need exactly-once behavior, you must
     * ensure idempotence on the application level.
     * <p>
     * IO failures are generally handled by Kafka producer and do not cause the
     * processor to fail. Refer to Kafka documentation for details.
     * <p>
     * Default local parallelism for this processor is 2 (or less if less CPUs
     * are available).
     *
     * @param properties     producer properties which should contain broker
     *                       address and key/value serializers
     * @param toRecordFn   function that extracts the key from the stream item
     *
     * @param <E> type of stream item
     * @param <K> type of the key published to Kafka
     * @param <V> type of the value published to Kafka
     */
    @Nonnull
    public static <E, K, V> Sink<E> kafka(
            @Nonnull Properties properties,
            @Nonnull DistributedFunction<? super E, ProducerRecord<K, V>> toRecordFn
    ) {
        return Sinks.fromProcessor("writeKafka", writeKafkaP(properties, toRecordFn));
    }

    /**
     * Convenience for {@link #kafka(Properties, DistributedFunction)} which creates
     * a {@code ProducerRecord} using the given topic and the given key and value
     * mapping functions
     *
     * @param <E> type of stream item
     * @param <K> type of the key published to Kafka
     * @param <V> type of the value published to Kafka
     * @param properties     producer properties which should contain broker
*                       address and key/value serializers
     * @param topic          name of the Kafka topic to publish to
     * @param extractKeyFn   function that extracts the key from the stream item
     * @param extractValueFn function that extracts the value from the stream item
*
     */
    @Nonnull
    public static <E, K, V> Sink<E> kafka(
            @Nonnull Properties properties,
            @Nonnull String topic,
            @Nonnull DistributedFunction<? super E, K> extractKeyFn,
            @Nonnull DistributedFunction<? super E, V> extractValueFn
    ) {
        return Sinks.fromProcessor("writeKafka", writeKafkaP(properties, topic, extractKeyFn, extractValueFn));
    }

    /**
     * Convenience for {@link #kafka(Properties, String, DistributedFunction, DistributedFunction)}
     * which expects {@code Map.Entry<K, V>} as input and extracts its key and value
     * parts to be published to Kafka.
     *
     * @param <K>        type of the key published to Kafka
     * @param <V>        type of the value published to Kafka
     * @param properties producer properties which should contain broker
     *                   address and key/value serializers
     * @param topic      Kafka topic name to publish to
     */
    @Nonnull
    public static <K, V> Sink<Entry<K, V>> kafka(@Nonnull Properties properties, @Nonnull String topic) {
        return kafka(properties, topic, Entry::getKey, Entry::getValue);
    }
}
