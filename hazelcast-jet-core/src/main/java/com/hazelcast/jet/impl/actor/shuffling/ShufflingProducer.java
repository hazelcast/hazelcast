/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.impl.actor.shuffling;

import com.hazelcast.jet.impl.actor.Producer;
import com.hazelcast.jet.impl.actor.ProducerCompletionHandler;

public class ShufflingProducer implements Producer {
    private final Producer producer;

    public ShufflingProducer(Producer producer) {
        this.producer = producer;
    }

    @Override
    public Object[] produce() throws Exception {
        return this.producer.produce();
    }

    @Override
    public int lastProducedCount() {
        return this.producer.lastProducedCount();
    }

    @Override
    public String getName() {
        return producer.getName();
    }

    @Override
    public boolean isClosed() {
        return producer.isClosed();
    }

    @Override
    public void open() {
        producer.open();
    }

    @Override
    public void close() {
        producer.close();
    }

    @Override
    public void registerCompletionHandler(ProducerCompletionHandler runnable) {
        producer.registerCompletionHandler(runnable);
    }

    @Override
    public void handleProducerCompleted() {
        producer.handleProducerCompleted();
    }
}
