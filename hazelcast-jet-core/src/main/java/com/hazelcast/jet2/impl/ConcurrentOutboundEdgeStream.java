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

import com.hazelcast.internal.util.concurrent.ConcurrentConveyor;
import com.hazelcast.util.Preconditions;

import java.util.ArrayList;
import java.util.Arrays;

import static com.hazelcast.jet2.impl.DoneItem.DONE_ITEM;
import static com.hazelcast.jet2.impl.ProgressState.DONE;

/**
 * Javadoc pending.
 */
class ConcurrentOutboundEdgeStream implements OutboundEdgeStream {
    private final int queueIndex;
    private final int ordinal;

    private final ProgressTracker tracker = new ProgressTracker();
    private final CircularCursor<ConcurrentConveyor<Object>> cursor;

    public ConcurrentOutboundEdgeStream(ConcurrentConveyor<Object>[] conveyors, int queueIndex, int ordinal) {
        Preconditions.checkTrue(conveyors.length > 0, "There must be at least one conveyor in the array");
        Preconditions.checkTrue(queueIndex >= 0, "queue index must be positive");
        Preconditions.checkTrue(queueIndex < conveyors[0].queueCount(),
                "Queue index must be less than number of queues in each conveyor");

        this.queueIndex = queueIndex;
        this.ordinal = ordinal;
        this.cursor = new CircularCursor<>(new ArrayList<>(Arrays.asList(conveyors)));
    }

    @Override
    public ProgressState offer(Object item) {
        if (item == DONE_ITEM) {
            return complete();
        }

        // use round robin to find a queue to put items to
        ConcurrentConveyor<Object> first = cursor.value();
        do {
            boolean offered = cursor.value().offer(queueIndex, item);
            cursor.advance();
            if (offered) {
                return DONE;
            }
        } while (cursor.value() != first);
        return ProgressState.NO_PROGRESS;
    }

    /**
     * Signal all conveyors with the gone item
     */
    private ProgressState complete() {
        tracker.reset();
        ConcurrentConveyor<Object> first = cursor.value();
        do {
            ConcurrentConveyor<Object> conveyor = cursor.value();
            if (conveyor.offer(queueIndex, conveyor.submitterGoneItem())) {
                tracker.update(DONE);
                cursor.remove();
            } else {
                tracker.notDone();
            }
        } while (cursor.advance() && cursor.value() != first);
        return tracker.toProgressState();
    }

    @Override
    public int ordinal() {
        return ordinal;
    }

}
