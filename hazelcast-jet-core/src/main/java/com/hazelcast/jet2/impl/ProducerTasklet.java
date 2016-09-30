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

public class ProducerTasklet<O> implements Tasklet {

    private final Producer<? extends O> producer;
    private final ArrayListCollector<O> collector;
    private Cursor<? extends O> collectorCursor;
    private final Cursor<QueueTail<? super O>> queueTailCursor;
    private boolean complete;

    public ProducerTasklet(Producer<? extends O> producer, Map<String, QueueTail<? super O>> queueTails) {
        Preconditions.checkNotNull(producer, "producer");
        Preconditions.checkFalse(queueTails.isEmpty(), "There must be at least one output");

        this.producer = producer;
        this.queueTailCursor = new ListCursor<>(new ArrayList<>(queueTails.values()));
        this.collector = new ArrayListCollector<>();
    }

    @Override
    public TaskletResult call() {
        boolean didPendingWork = false;
        if (collectorCursor != null) {
            switch (offerPendingOutput()) {
                case ACCEPTED_NONE:
                    return TaskletResult.NO_PROGRESS;
                case ACCEPTED_SOME:
                    return TaskletResult.MADE_PROGRESS;
                case ACCEPTED_ALL:
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
        OfferResult result = offerPendingOutput();
        return (complete && result == OfferResult.ACCEPTED_ALL) ? TaskletResult.DONE : TaskletResult.MADE_PROGRESS;
    }

    @Override
    public boolean isBlocking() {
        return producer.isBlocking();
    }

    private OfferResult offerPendingOutput() {
        boolean pushedSome = false;
        do {
            do {
                boolean pushed = queueTailCursor.value().offer(collectorCursor.value());
                pushedSome |= pushed;
                if (!pushed) {
                    return pushedSome ? OfferResult.ACCEPTED_SOME : OfferResult.ACCEPTED_NONE;
                }
            } while (queueTailCursor.advance());
            queueTailCursor.reset();
        } while (collectorCursor.advance());

        collector.clear();
        collectorCursor = null;
        return OfferResult.ACCEPTED_ALL;
    }

    private enum OfferResult {
        ACCEPTED_ALL,
        ACCEPTED_SOME,
        ACCEPTED_NONE,
    }
}

