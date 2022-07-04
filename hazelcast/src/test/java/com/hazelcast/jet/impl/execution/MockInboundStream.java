/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.jet.core.Watermark;
import com.hazelcast.jet.impl.util.ProgressState;

import javax.annotation.Nonnull;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Deque;
import java.util.List;
import java.util.function.Predicate;

import static com.hazelcast.jet.impl.execution.DoneItem.DONE_ITEM;
import static com.hazelcast.jet.impl.util.ProgressState.DONE;
import static com.hazelcast.jet.impl.util.ProgressState.MADE_PROGRESS;
import static com.hazelcast.jet.impl.util.ProgressState.NO_PROGRESS;
import static com.hazelcast.jet.impl.util.ProgressState.WAS_ALREADY_DONE;

public class MockInboundStream implements InboundEdgeStream {
    private int ordinal;
    private int priority;
    private final Deque<Object> mockData;
    private final int chunkSize;

    private boolean done;

    MockInboundStream(int priority, List<?> mockData, int chunkSize) {
        this.priority = priority;
        this.chunkSize = chunkSize;
        this.mockData = new ArrayDeque<>(mockData);
    }

    void push(Object... items) {
        mockData.addAll(Arrays.asList(items));
    }

    public void setOrdinal(int ordinal) {
        this.ordinal = ordinal;
    }

    @Nonnull @Override
    public ProgressState drainTo(@Nonnull Predicate<Object> dest) {
        if (done) {
            return WAS_ALREADY_DONE;
        }
        if (mockData.isEmpty()) {
            return NO_PROGRESS;
        }
        for (int i = 0; i < chunkSize && !mockData.isEmpty(); i++) {
            final Object item = mockData.poll();
            if (item == DONE_ITEM) {
                done = true;
                break;
            } else if (!dest.test(item) || item instanceof SnapshotBarrier || item instanceof Watermark) {
                break;
            }
        }
        return done ? DONE : MADE_PROGRESS;
    }

    @Override
    public boolean isDone() {
        return done;
    }

    @Override
    public int ordinal() {
        return ordinal;
    }

    @Override
    public int priority() {
        return priority;
    }

    public Deque<Object> remainingItems() {
        return mockData;
    }

    @Override
    public int sizes() {
        return mockData.size();
    }

    @Override
    public int capacities() {
        return Integer.MAX_VALUE;
    }

    @Override
    public long topObservedWm() {
        return 0;
    }

    @Override
    public long coalescedWm() {
        return 0;
    }
}
