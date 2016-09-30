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

package com.hazelcast.jet2.impl;

import com.hazelcast.jet2.Cursor;
import com.hazelcast.jet2.Producer;
import com.hazelcast.util.Preconditions;

import java.util.ArrayList;
import java.util.Map;

public class ProducerTasklet<T> implements Tasklet {

    private final Producer<? extends T> producer;
    private final ArrayListCollector<T> collector;
    private Cursor<T> collectorCursor;
    private final Cursor<QueueTail<T>> outputCursor;
    private boolean complete;

    public ProducerTasklet(Producer<? extends T> producer, Map<String, QueueTail<T>> outputs) {
        Preconditions.checkNotNull(producer, "producer");
        Preconditions.checkFalse(outputs.isEmpty(), "There must be at least one output");

        this.producer = producer;
        this.outputCursor = new ListCursor<>(new ArrayList<>(outputs.values()));
        outputCursor.advance();
        this.collector = new ArrayListCollector<>();
    }

    @Override
    public TaskletResult call() {
        boolean didPendingWork = false;
        if (collectorCursor != null) {
            switch (tryOffer()) {
                case OFFERED_NONE:
                    return TaskletResult.NO_PROGRESS;
                case OFFERED_SOME:
                    return TaskletResult.MADE_PROGRESS;
                case OFFERED_ALL:
                    if (complete) {
                        return TaskletResult.DONE;
                    }
                    didPendingWork = true;
                    break;
            }
        }

        complete = producer.produce(collector);
        if (complete && collector.isEmpty()) {
            return TaskletResult.DONE;
        }
        if (collector.isEmpty()) {
            return didPendingWork ? TaskletResult.MADE_PROGRESS : TaskletResult.NO_PROGRESS;
        }

        collectorCursor = collector.cursor();
        collectorCursor.reset();
        collectorCursor.advance();
        OfferResult result = tryOffer();
        return (complete && result == OfferResult.OFFERED_ALL) ? TaskletResult.DONE : TaskletResult.MADE_PROGRESS;
    }

    @Override
    public boolean isBlocking() {
        return producer.isBlocking();
    }

    private OfferResult tryOffer() {
        boolean pushedSome = false;
        do {
            do {
                boolean pushed = outputCursor.value().offer(collectorCursor.value());
                pushedSome |= pushed;
                if (!pushed) {
                    return pushedSome ? OfferResult.OFFERED_SOME : OfferResult.OFFERED_NONE;
                }
            } while (outputCursor.advance());
            outputCursor.reset();
            outputCursor.advance();
        } while (collectorCursor.advance());

        collector.clear();
        collectorCursor = null;
        return OfferResult.OFFERED_ALL;
    }

    private enum OfferResult {
        OFFERED_ALL,
        OFFERED_SOME,
        OFFERED_NONE,
    }
}

