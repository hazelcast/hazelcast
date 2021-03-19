/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.impl.execution;

import com.hazelcast.internal.util.concurrent.ConcurrentConveyor;
import com.hazelcast.jet.impl.util.ProgressState;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import static java.util.Objects.requireNonNull;

public class ConveyorCollector implements OutboundCollector {

    private final ConcurrentConveyor<Object> conveyor;
    private final int queueIndex;
    private final int[] partitions;

    public ConveyorCollector(@Nonnull ConcurrentConveyor<Object> conveyor, int queueIndex, @Nullable int[] partitions) {
        this.conveyor = requireNonNull(conveyor);
        this.queueIndex = queueIndex;
        this.partitions = partitions;
    }

    @Override
    public ProgressState offer(Object item) {
        return offerToConveyor(item);
    }

    @Override
    public ProgressState offerBroadcast(BroadcastItem item) {
        return offerToConveyor(item);
    }

    @Override
    public int[] getPartitions() {
        return partitions;
    }

    protected ProgressState offerToConveyor(Object item) {
        return conveyor.offer(queueIndex, item) ? ProgressState.DONE : ProgressState.NO_PROGRESS;
    }
}

