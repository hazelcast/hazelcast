/*
 * Copyright 2023 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.jet.kafka.impl;

import org.apache.kafka.clients.producer.KafkaProducer;

import java.time.Duration;
import java.util.Properties;

public class NonClosingKafkaProducer<K, V> extends KafkaProducer<K, V> {

    private final Runnable onClose;

    public NonClosingKafkaProducer(Properties properties, Runnable onClose) {
        super(properties);
        this.onClose = onClose;
    }

    public void doClose() {
        // close() without parameters delegates to this, which we override below
        super.close(Duration.ofMillis(Long.MAX_VALUE));
    }

    @Override
    public void close() {
        onClose.run();
    }

    @Override
    public void close(Duration timeout) {
        onClose.run();
    }

}
