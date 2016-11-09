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
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

class ConveyorCollector implements OutboundCollector {

    protected final Object doneItem;

    private final ConcurrentConveyor<Object> conveyor;
    private final int queueIndex;
    private final int[] partitions;

    @SuppressFBWarnings("EI_EXPOSE_REP")
    ConveyorCollector(ConcurrentConveyor<Object> conveyor, int queueIndex, int[] partitions) {
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
    @SuppressFBWarnings("EI_EXPOSE_REP")
    public int[] getPartitions() {
        return partitions;
    }
}

class ConveyorCollectorWithPartition extends ConveyorCollector {

    ConveyorCollectorWithPartition(ConcurrentConveyor<Object> conveyor, int queueIndex, int[] partitions) {
        super(conveyor, queueIndex, partitions);
    }

    @Override
    public ProgressState offer(Object item, int partitionId) {
        return super.offer(new ObjectWithPartitionId(item, partitionId));
    }

    @Override
    public ProgressState offer(Object item) {
        return offer(item, -1);
    }

    @Override
    public ProgressState close() {
        return super.offer(doneItem);
    }
}
