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

/**
 * Javadoc pending.
 */
class ConcurrentOutboundEdgeStream implements OutboundEdgeStream {
    private final ConcurrentConveyor<Object>[] conveyors;
    private final int queueIndex;
    private final int ordinal;

    private int nextIndex = -1;

    public ConcurrentOutboundEdgeStream(ConcurrentConveyor<Object>[] conveyors, int queueIndex, int ordinal) {
        Preconditions.checkTrue(conveyors.length > 0, "There must be at least one conveyor in the array");
        Preconditions.checkTrue(queueIndex >= 0, "queue index must be positive");
        Preconditions.checkTrue(queueIndex < conveyors[0].queueCount(),
                "Queue index must be less than number of queues in each conveyor");

        this.queueIndex = queueIndex;
        this.ordinal = ordinal;
        this.conveyors = conveyors;
    }

    @Override
    public boolean offer(Object item) {
        if (++nextIndex >= conveyors.length) {
            nextIndex = 0;
        }
        return conveyors[nextIndex].offer(queueIndex, item);
    }

    @Override
    public int ordinal() {
        return ordinal;
    }
}
