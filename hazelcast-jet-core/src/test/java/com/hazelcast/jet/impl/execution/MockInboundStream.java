/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.jet.impl.util.ProgressState;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;

import static com.hazelcast.jet.impl.execution.DoneItem.DONE_ITEM;
import static com.hazelcast.jet.impl.util.ProgressState.DONE;
import static com.hazelcast.jet.impl.util.ProgressState.MADE_PROGRESS;
import static com.hazelcast.jet.impl.util.ProgressState.NO_PROGRESS;
import static com.hazelcast.jet.impl.util.ProgressState.WAS_ALREADY_DONE;

public class MockInboundStream implements InboundEdgeStream {
    private int ordinal;
    private int priority;
    private final List<Object> mockData;
    private final int chunkSize;

    private int dataIndex;
    private boolean done;

    MockInboundStream(int priority, List<?> mockData, int chunkSize) {
        this.priority = priority;
        this.chunkSize = chunkSize;
        this.mockData = new ArrayList<>(mockData);
    }

    void push(Object... items) {
        mockData.addAll(Arrays.asList(items));
    }

    public void setOrdinal(int ordinal) {
        this.ordinal = ordinal;
    }

    @Override
    public ProgressState drainTo(Consumer<Object> dest) {
        if (done) {
            return WAS_ALREADY_DONE;
        }
        if (dataIndex == mockData.size()) {
            return NO_PROGRESS;
        }
        final int limit = Math.min(mockData.size(), dataIndex + chunkSize);
        for (; dataIndex < limit; dataIndex++) {
            final Object item = mockData.get(dataIndex);
            if (item == DONE_ITEM) {
                done = true;
                break;
            } else if (item instanceof SnapshotBarrier) {
                dest.accept(item);
                dataIndex++;
                break;
            } else {
                dest.accept(item);
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
}
