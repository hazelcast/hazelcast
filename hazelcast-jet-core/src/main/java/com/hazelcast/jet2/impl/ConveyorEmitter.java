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

import java.util.List;
import java.util.function.Predicate;

class ConveyorEmitter implements InboundEmitter {

    private final ConcurrentConveyor<Object> conveyor;
    private final int queueIndex;
    private final List<Integer> partitions;
    private final DoneDetector doneDetector;

    public ConveyorEmitter(ConcurrentConveyor<Object> conveyor, int queueIndex, List<Integer> partitions) {
        this.conveyor = conveyor;
        this.queueIndex = queueIndex;
        this.partitions = partitions;
        doneDetector = new DoneDetector(conveyor.submitterGoneItem());
    }

    public ProgressState drainTo(CollectionWithObserver dest) {
        dest.setVetoingObserverOfAdd(doneDetector);
        int numDrained;
        try {
            numDrained = conveyor.drainTo(queueIndex, dest);
        } finally {
            dest.setVetoingObserverOfAdd(null);
        }
        return ProgressState.valueOf(numDrained > 0, doneDetector.done);
    }

    private final class DoneDetector implements Predicate<Object> {
        final Object doneItem;
        boolean done;

        public DoneDetector(Object doneItem) {
            this.doneItem = doneItem;
        }

        @Override
        public boolean test(Object o) {
            if (o == doneItem) {
                assert !done : "Repeated 'submitterGoneItem' in queue at index " + queueIndex;
                done = true;
                return false;
            }
            return true;
        }
    }
}
