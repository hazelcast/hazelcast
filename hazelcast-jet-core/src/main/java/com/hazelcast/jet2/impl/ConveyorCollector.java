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

public class ConveyorCollector implements OutboundCollector {

    private final ConcurrentConveyor<Object> conveyor;
    private final int queueIndex;
    private final List<Integer> partitions;
    private final Object doneItem;

    public ConveyorCollector(ConcurrentConveyor<Object> conveyor, int queueIndex, List<Integer> partitions) {
        this.conveyor = conveyor;
        this.queueIndex = queueIndex;
        this.partitions = partitions;
        this.doneItem = conveyor.submitterGoneItem();
    }

    @Override
    public ProgressState offer(Object item) {
        return conveyor.offer(queueIndex, item) ? ProgressState.DONE : ProgressState.NO_PROGRESS;
    }

    @Override
    public ProgressState close() {
        return offer(doneItem);
    }

    @Override
    public List<Integer> getPartitions() {
        return partitions;
    }
}
