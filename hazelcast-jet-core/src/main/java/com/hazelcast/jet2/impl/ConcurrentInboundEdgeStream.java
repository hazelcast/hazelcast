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

import java.util.function.Predicate;

/**
 * Javadoc pending.
 */
class ConcurrentInboundEdgeStream implements InboundEdgeStream {

    private final ConcurrentConveyor<Object> conveyor;
    private final int ordinal;
    private final int priority;
    private final ExhaustedQueueCleaner exhaustedQueueCleaner = new ExhaustedQueueCleaner();

    public ConcurrentInboundEdgeStream(ConcurrentConveyor<Object> conveyor, int ordinal, int priority) {
        this.conveyor = conveyor;
        this.ordinal = ordinal;
        this.priority = priority;
    }

    @Override
    public ProgressState drainAvailableItemsInto(CollectionWithPredicate dest) {
        assert dest.isEmpty() : "Destination is not empty";
        boolean madeProgress = false;
        dest.setPredicateOfAdd(exhaustedQueueCleaner);
        try {
            exhaustedQueueCleaner.doneItem = conveyor.submitterGoneItem();
            for (int i = 0; i < conveyor.queueCount(); i++) {
                if (conveyor.queue(i) == null) {
                    continue;
                }
                exhaustedQueueCleaner.index = i;
                madeProgress |= conveyor.drainTo(i, dest) > 0;
            }
        } finally {
            dest.setPredicateOfAdd(null);
        }
        return ProgressState.valueOf(madeProgress, exhaustedQueueCleaner.cleanedCount == conveyor.queueCount());
    }

    @Override
    public int ordinal() {
        return ordinal;
    }

    @Override
    public int priority() {
        return priority;
    }

    private final class ExhaustedQueueCleaner implements Predicate<Object> {
        Object doneItem;
        int index;
        int cleanedCount;

        @Override
        public boolean test(Object o) {
            if (o == doneItem) {
                assert conveyor.queue(index) != null : "Repeated 'submitterGoneItem' in queue at index " + index;
                conveyor.removeQueue(index);
                cleanedCount++;
                return false;
            }
            return true;
        }
    }
}
