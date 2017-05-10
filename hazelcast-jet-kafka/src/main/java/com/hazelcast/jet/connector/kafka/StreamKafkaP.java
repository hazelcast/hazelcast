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

package com.hazelcast.jet.connector.kafka;

import com.hazelcast.jet.AbstractProcessor;
import com.hazelcast.jet.function.DistributedSupplier;
import com.hazelcast.jet.Processor;
import com.hazelcast.util.Preconditions;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import javax.annotation.Nonnull;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;

import static com.hazelcast.jet.Util.entry;

/**
 * Kafka reader for Jet, emits records read from Kafka as {@code Map.Entry}.
 */
public final class StreamKafkaP extends AbstractProcessor {

    private static final int POLL_TIMEOUT_MS = 1000;
    private final Properties properties;
    private final String[] topicIds;
    private CompletableFuture<Void> jobFuture;

    private StreamKafkaP(String[] topicIds, Properties properties) {
        this.topicIds = topicIds;
        this.properties = properties;
    }

    /**
     * Returns a 2supplier of processors that consume one or more Kafka topics and emit
     * items from it as {@code Map.Entry} instances.
     * <p>
     * A {@code KafkaConsumer} is created per {@code Processor} instance using the
     * supplied properties. All processors must be in the same consumer group
     * supplied by the {@code group.id} property.
     * The supplied properties will be passed on to the {@code KafkaConsumer} instance.
     * These processors are only terminated in case of an error or if the underlying job is cancelled.
     *
     * @param topics     the list of topics
     * @param properties consumer properties which should contain consumer group name,
     *                   broker address and key/value deserializers
     */
    public static DistributedSupplier<Processor> streamKafka(Properties properties, String... topics) {
        Preconditions.checkPositive(topics.length, "At least one topic must be supplied");
        Preconditions.checkTrue(properties.containsKey("group.id"), "Properties should contain `group.id`");
        properties.put("enable.auto.commit", false);

        return () -> new StreamKafkaP(topics, properties);
    }

    @Override
    protected void init(@Nonnull Context context) throws Exception {
        jobFuture = context.jobFuture();
    }

    @Override
    public boolean complete() {
        try (KafkaConsumer<?, ?> consumer = new KafkaConsumer<>(properties)) {
            consumer.subscribe(Arrays.asList(topicIds));

            while (!jobFuture.isDone()) {
                ConsumerRecords<?, ?> records = consumer.poll(POLL_TIMEOUT_MS);

                for (ConsumerRecord<?, ?> r : records) {
                    emit(entry(r.key(), r.value()));
                }
                consumer.commitSync();
            }
        }

        return true;
    }

    @Override
    public boolean isCooperative() {
        return false;
    }
}
