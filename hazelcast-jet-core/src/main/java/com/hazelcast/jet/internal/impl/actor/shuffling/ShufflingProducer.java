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

package com.hazelcast.jet.internal.impl.actor.shuffling;

import com.hazelcast.jet.internal.api.actor.ObjectProducer;
import com.hazelcast.jet.internal.api.actor.ProducerCompletionHandler;
import com.hazelcast.jet.api.dag.Vertex;
import com.hazelcast.jet.api.strategy.DataTransferringStrategy;

public class ShufflingProducer implements ObjectProducer {
    private final ObjectProducer objectProducer;

    public ShufflingProducer(ObjectProducer objectProducer) {
        this.objectProducer = objectProducer;
    }

    @Override
    public Object[] produce() throws Exception {
        return this.objectProducer.produce();
    }

    @Override
    public int lastProducedCount() {
        return this.objectProducer.lastProducedCount();
    }

    @Override
    public boolean isShuffled() {
        return true;
    }

    @Override
    public Vertex getVertex() {
        return objectProducer.getVertex();
    }

    @Override
    public String getName() {
        return objectProducer.getName();
    }

    @Override
    public boolean isClosed() {
        return objectProducer.isClosed();
    }

    @Override
    public void open() {
        objectProducer.open();
    }

    @Override
    public void close() {
        objectProducer.close();
    }

    @Override
    public DataTransferringStrategy getDataTransferringStrategy() {
        return objectProducer.getDataTransferringStrategy();
    }

    @Override
    public void registerCompletionHandler(ProducerCompletionHandler runnable) {
        objectProducer.registerCompletionHandler(runnable);
    }

    @Override
    public void handleProducerCompleted() {
        objectProducer.handleProducerCompleted();
    }
}
