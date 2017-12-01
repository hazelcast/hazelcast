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

import com.hazelcast.jet.core.Inbox;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.core.Watermark;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Stream;

import static com.hazelcast.jet.impl.util.ExceptionUtil.sneakyThrow;
import static java.util.stream.Collectors.toList;

/**
 * See {@link com.hazelcast.jet.core.processor.KafkaProcessors#writeKafkaP(
 * Properties, String, com.hazelcast.jet.function.DistributedFunction,
 * com.hazelcast.jet.function.DistributedFunction)
 * KafkaProcessors.writeKafka()}.
 */
public final class WriteKafkaP<T, K, V> implements Processor {

    private final KafkaProducer<K, V> producer;
    private final Function<T, ProducerRecord<K, V>> toRecordFn;
    private final AtomicReference<Throwable> lastError = new AtomicReference<>();

    private final Callback callback = (metadata, exception) -> {
        // Note: this method may be called on different thread.
        if (exception != null) {
            lastError.compareAndSet(null, exception);
        }
    };

    WriteKafkaP(KafkaProducer<K, V> producer, Function<T, ProducerRecord<K, V>> toRecordFn) {
        this.producer = producer;
        this.toRecordFn = toRecordFn;
    }

    @Override
    public boolean isCooperative() {
        return false;
    }

    @Override
    public boolean tryProcess() {
        checkError();
        return true;
    }

    @Override
    public void process(int ordinal, @Nonnull Inbox inbox) {
        checkError();
        inbox.drain((Object item) -> {
            if (!(item instanceof Watermark)) {
                // Note: send() method can block even though it is declared to not. This is true for Kafka 1.0 and probably
                // will stay so, unless they change API.
                producer.send(toRecordFn.apply((T) item), callback);
            }
        });
    }

    @Override
    public boolean complete() {
        ensureAllWritten();
        return true;
    }

    @Override
    public boolean saveToSnapshot() {
        ensureAllWritten();
        return true;
    }

    private void ensureAllWritten() {
        checkError();
        // flush() should ensure that all lingering records are sent and that all futures from
        // producer.send() are done.
        producer.flush();
    }

    private void checkError() {
        Throwable t = lastError.get();
        if (t != null) {
            throw sneakyThrow(t);
        }
    }

    public static class Supplier<T, K, V> implements ProcessorSupplier {

        private static final long serialVersionUID = 1L;

        private final Properties properties;
        private final Function<? super T, ProducerRecord<K, V>> toRecordFn;

        private transient KafkaProducer<K, V> producer;

        public Supplier(Properties properties, Function<? super T, ProducerRecord<K, V>> toRecordFn) {
            this.properties = properties;
            this.toRecordFn = toRecordFn;
        }

        @Override
        public void init(@Nonnull Context context) {
            producer = new KafkaProducer<>(properties);
        }

        @Override @Nonnull
        public List<Processor> get(int count) {
            return Stream.generate(() -> new WriteKafkaP<>(producer, toRecordFn))
                         .limit(count)
                         .collect(toList());
        }

        @Override
        public void complete(Throwable error) {
            if (producer != null) {
                producer.close();
            }
        }
    }
}
