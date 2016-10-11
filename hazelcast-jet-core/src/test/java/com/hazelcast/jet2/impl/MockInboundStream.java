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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static com.hazelcast.jet2.impl.TaskletResult.DONE;
import static com.hazelcast.jet2.impl.TaskletResult.MADE_PROGRESS;
import static com.hazelcast.jet2.impl.TaskletResult.NO_PROGRESS;
import static com.hazelcast.jet2.impl.TaskletResult.WAS_ALREADY_DONE;

public class MockInboundStream implements InboundEdgeStream {

    private static final Object DONE_ITEM = new Object();

    private final int chunkSize;
    private final List<Object> mockData;
    private final int ordinal;
    private int dataIndex;
    private boolean paused;
    private boolean done;

    public MockInboundStream(List<?> mockData, int chunkSize) {
        this(0, mockData, chunkSize);
    }

    public MockInboundStream(int ordinal, List<?> mockData, int chunkSize) {
        this.ordinal = ordinal;
        this.chunkSize = chunkSize;
        this.mockData = new ArrayList<>(mockData);
        this.dataIndex = 0;
    }

    public void push(Object... items) {
        mockData.addAll(Arrays.asList(items));
    }

    public void pushDoneItem() {
        mockData.add(DONE_ITEM);
    }

    @Override
    public TaskletResult drainAvailableItemsInto(CollectionWithPredicate dest) {
        if (done) {
            return WAS_ALREADY_DONE;
        }
        if (paused) {
            return NO_PROGRESS;
        }
        final int limit = Math.min(mockData.size(), dataIndex + chunkSize);
        dest.setPredicateOfAdd(x -> {
        });
        for (; dataIndex < limit; dataIndex++) {
            final Object item = mockData.get(dataIndex);
            if (item != DONE_ITEM) {
                assert !done : "DONE_ITEM followed by more items";
                dest.add(item);
            } else {
                done = true;
            }
        }
        dest.setPredicateOfAdd(null);
        return done ? DONE : MADE_PROGRESS;
    }

    @Override
    public int ordinal() {
        return ordinal;
    }

    @Override
    public int priority() {
        return 0;
    }

    void pause() {
        paused = true;
    }

    boolean isDone() {
        return done;
    }
}
