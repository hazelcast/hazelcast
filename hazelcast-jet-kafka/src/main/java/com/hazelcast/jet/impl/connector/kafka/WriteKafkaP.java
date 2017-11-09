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

import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.ProcessorSupplier;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Properties;
import java.util.function.Function;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;

/**
 * See {@link com.hazelcast.jet.core.processor.KafkaProcessors#writeKafkaP(
 * Properties, String, com.hazelcast.jet.function.DistributedFunction,
 * com.hazelcast.jet.function.DistributedFunction)
 * KafkaProcessors.writeKafka()}.
 */
public final class WriteKafkaP<T, K, V> extends AbstractProcessor {

    private final KafkaProducer<K, V> producer;
    private final Function<T, ProducerRecord<K, V>> toRecordFn;

    WriteKafkaP(KafkaProducer<K, V> producer, Function<T, ProducerRecord<K, V>> toRecordFn) {
        this.producer = producer;
        this.toRecordFn = toRecordFn;
    }

    @Override
    public boolean isCooperative() {
        return false;
    }

    @Override
    @SuppressWarnings("unchecked")
    protected boolean tryProcess(int ordinal, @Nonnull Object item) throws Exception {
        producer.send(toRecordFn.apply((T) item));
        return true;
    }

    @Override
    public boolean complete() {
        producer.flush();
        return true;
    }

    public static class Supplier<T, K, V> implements ProcessorSupplier {

        static final long serialVersionUID = 1L;

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
