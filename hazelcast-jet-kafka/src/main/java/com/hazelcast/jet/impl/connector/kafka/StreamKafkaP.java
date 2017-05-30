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

package com.hazelcast.jet.impl.connector.kafka;

import com.hazelcast.jet.AbstractProcessor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import javax.annotation.Nonnull;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;

import static com.hazelcast.jet.Util.entry;

/**
 * See {@link com.hazelcast.jet.processor.KafkaProcessors#streamKafka(
 * Properties, String...)}.
 */
public final class StreamKafkaP extends AbstractProcessor {

    private static final int POLL_TIMEOUT_MS = 1000;
    private final Properties properties;
    private final String[] topicIds;
    private CompletableFuture<Void> jobFuture;

    public StreamKafkaP(Properties properties, String[] topicIds) {
        this.properties = properties;
        this.topicIds = Arrays.copyOf(topicIds, topicIds.length);
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
