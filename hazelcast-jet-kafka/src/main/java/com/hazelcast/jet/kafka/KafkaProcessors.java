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

import com.hazelcast.function.FunctionEx;
import com.hazelcast.internal.util.Preconditions;
import com.hazelcast.jet.core.EventTimePolicy;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.kafka.impl.StreamKafkaP;
import com.hazelcast.jet.kafka.impl.WriteKafkaP;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nonnull;
import java.util.Arrays;
import java.util.Properties;

/**
 * Static utility class with factories of Apache Kafka source and sink
 * processors.
 *
 * @since 3.0
 */
public final class KafkaProcessors {

    private static final int PREFERRED_LOCAL_PARALLELISM = 4;

    private KafkaProcessors() {
    }

    /**
     * Returns a supplier of processors for {@link
     * KafkaSources#kafka(Properties, FunctionEx, String...)}.
     */
    public static <K, V, T> ProcessorMetaSupplier streamKafkaP(
            @Nonnull Properties properties,
            @Nonnull FunctionEx<? super ConsumerRecord<K, V>, ? extends T> projectionFn,
            @Nonnull EventTimePolicy<? super T> eventTimePolicy,
            @Nonnull String... topics
    ) {
        Preconditions.checkPositive(topics.length, "At least one topic must be supplied");
        properties.put("enable.auto.commit", false);
        return ProcessorMetaSupplier.of(
                PREFERRED_LOCAL_PARALLELISM,
                StreamKafkaP.processorSupplier(properties, Arrays.asList(topics), projectionFn, eventTimePolicy)
        );
    }

    /**
     * Returns a supplier of processors for
     * {@link KafkaSinks#kafka(Properties, String, FunctionEx, FunctionEx)}.
     */
    public static <T, K, V> ProcessorMetaSupplier writeKafkaP(
            @Nonnull Properties properties,
            @Nonnull String topic,
            @Nonnull FunctionEx<? super T, ? extends K> extractKeyFn,
            @Nonnull FunctionEx<? super T, ? extends V> extractValueFn
    ) {
        return writeKafkaP(properties, (T t) ->
                new ProducerRecord<>(topic, extractKeyFn.apply(t), extractValueFn.apply(t))
        );
    }

    /**
     * Returns a supplier of processors for
     * {@link KafkaSinks#kafka(Properties, FunctionEx)}.
     */
    public static <T, K, V> ProcessorMetaSupplier writeKafkaP(
            @Nonnull Properties properties,
            @Nonnull FunctionEx<? super T, ? extends ProducerRecord<K, V>> toRecordFn
    ) {
        return ProcessorMetaSupplier.of(2, new WriteKafkaP.Supplier<T, K, V>(properties, toRecordFn));
    }
}
