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
import com.hazelcast.jet.Distributed.Function;
import com.hazelcast.jet.Processor;
import com.hazelcast.jet.ProcessorMetaSupplier;
import com.hazelcast.jet.ProcessorSupplier;
import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.impl.util.Util;
import com.hazelcast.nio.Address;
import com.hazelcast.util.Preconditions;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import javax.annotation.Nonnull;
import java.io.Closeable;
import java.util.Arrays;
import java.util.List;
import java.util.Map.Entry;
import java.util.Properties;

import static com.hazelcast.jet.Traversers.traverseIterable;
import static com.hazelcast.jet.Util.entry;
import static java.util.stream.Collectors.toList;
import static java.util.stream.IntStream.range;


/**
 * Kafka reader for Jet, emits records read from Kafka as {@code Map.Entry}.
 *
 * @param <K> type of the message key
 * @param <V> type of the message value
 */
public final class ReadKafkaP<K, V> extends AbstractProcessor implements Closeable {

    private static final int POLL_TIMEOUT_MS = 100;
    private final Properties properties;
    private final String[] topicIds;
    private KafkaConsumer<K, V> consumer;
    private Traverser<Entry<K, V>> traverser;

    private ReadKafkaP(String[] topicIds, Properties properties) {
        this.topicIds = topicIds;
        this.properties = properties;

    }

    /**
     * Returns a meta-supplier of processors that consume one or more Kafka topics and emit
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
    public static ProcessorMetaSupplier readKafka(Properties properties, String... topics) {
        Preconditions.checkPositive(topics.length, "At least one topic must be supplied");
        Preconditions.checkTrue(properties.containsKey("group.id"), "Properties should contain `group.id`");
        properties.put("enable.auto.commit", false);

        return new MetaSupplier<>(topics, properties);
    }

    @Override
    protected void init(@Nonnull Context context) throws Exception {
        consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(Arrays.asList(topicIds));
        traverser = () -> null;
    }

    @Override
    public boolean isCooperative() {
        return false;
    }

    @Override
    public boolean complete() {
        if (emitCooperatively(traverser)) {
            consumer.commitSync();
            traverser = traverseIterable(consumer.poll(POLL_TIMEOUT_MS)).map(r -> entry(r.key(), r.value()));
        }
        return false;
    }

    @Override
    public void close() {
        consumer.close();
    }

    private static final class MetaSupplier<K, V> implements ProcessorMetaSupplier {

        static final long serialVersionUID = 1L;
        private final String[] topicIds;
        private Properties properties;

        private MetaSupplier(String[] topicIds, Properties properties) {
            this.topicIds = topicIds;
            this.properties = properties;
        }

        @Override @Nonnull
        public Function<Address, ProcessorSupplier> get(@Nonnull List<Address> addresses) {
            return address -> new Supplier<>(topicIds, properties);
        }
    }

    private static class Supplier<K, V> implements ProcessorSupplier {

        static final long serialVersionUID = 1L;

        private final String[] topicIds;
        private final Properties properties;
        private transient List<Processor> processors;

        Supplier(String[] topicIds, Properties properties) {
            this.properties = properties;
            this.topicIds = topicIds;
        }

        @Override @Nonnull
        public List<Processor> get(int count) {
            return processors = range(0, count)
                    .mapToObj(i -> new ReadKafkaP<>(topicIds, properties))
                    .collect(toList());
        }

        @Override
        public void complete(Throwable error) {
            processors.stream()
                      .filter(p -> p instanceof ReadKafkaP)
                      .map(p -> (ReadKafkaP) p)
                      .forEach(p -> Util.uncheckRun(p::close));
        }
    }
}
