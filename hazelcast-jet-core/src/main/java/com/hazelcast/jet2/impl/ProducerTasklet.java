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
    private final Cursor<Output<T>> outputCursor;
    private boolean complete;

    public ProducerTasklet(Producer<? extends T> producer, Map<String, Output<T>> outputs) {
        Preconditions.checkNotNull(producer, "producer");
        Preconditions.checkTrue(outputs.size() > 0, "There must be at least one output");

        this.producer = producer;
        this.outputCursor = new ListCursor<>(new ArrayList<>(outputs.values()));
        outputCursor.advance();
        this.collector = new ArrayListCollector<>();
    }

    @Override
    public TaskletResult call() {
        if (collectorCursor != null) {
            switch (tryPush()) {
                case PUSHED_NONE:
                    return TaskletResult.NO_PROGRESS;
                case PUSHED_SOME:
                    return TaskletResult.MADE_PROGRESS;
                case PUSHED_ALL:
                    if (complete) {
                        return TaskletResult.DONE;
                    }
                    break;
            }
        }

        complete = producer.produce(collector);
        if (complete && collector.isEmpty()) {
            return TaskletResult.DONE;
        }
        if (collector.isEmpty()) {
            return TaskletResult.NO_PROGRESS;
        }

        collectorCursor = collector.cursor();
        collectorCursor.reset();
        collectorCursor.advance();
        PushResult result = tryPush();
        return (complete && result == PushResult.PUSHED_ALL) ? TaskletResult.DONE : TaskletResult.MADE_PROGRESS;
    }

    @Override
    public boolean isBlocking() {
        return producer.isBlocking();
    }

    private PushResult tryPush() {
        boolean pushedSome = false;
        do {
            do {
                boolean pushed = outputCursor.value().collect(collectorCursor.value());
                pushedSome |= pushed;
                if (!pushed) {
                    return pushedSome ? PushResult.PUSHED_SOME : PushResult.PUSHED_NONE;
                }
            } while (outputCursor.advance());
            outputCursor.reset();
            outputCursor.advance();
        } while (collectorCursor.advance());

        collector.clear();
        collectorCursor = null;
        return PushResult.PUSHED_ALL;
    }

    private enum PushResult {
        PUSHED_ALL,
        PUSHED_SOME,
        PUSHED_NONE,
    }
}

