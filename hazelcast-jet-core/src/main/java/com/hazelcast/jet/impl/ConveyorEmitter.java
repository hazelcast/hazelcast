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

package com.hazelcast.jet.impl;

import com.hazelcast.internal.util.concurrent.ConcurrentConveyor;

import java.util.Collection;

class ConveyorEmitter implements InboundEmitter {

    private final CollectionWithDoneDetector doneDetector = new CollectionWithDoneDetector();
    private final ConcurrentConveyor<Object> conveyor;
    private final int queueIndex;

    ConveyorEmitter(ConcurrentConveyor<Object> conveyor, int queueIndex) {
        this.conveyor = conveyor;
        this.queueIndex = queueIndex;
    }

    @Override
    public ProgressState drainTo(Collection<Object> dest) {
        doneDetector.wrapped = dest;
        try {
            int drainedCount = conveyor.drainTo(queueIndex, doneDetector);
            return ProgressState.valueOf(drainedCount > 0, doneDetector.done);
        } finally {
            doneDetector.wrapped = null;
        }
    }

}
