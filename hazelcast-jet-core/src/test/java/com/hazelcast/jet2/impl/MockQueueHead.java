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

import com.hazelcast.jet2.Chunk;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import static com.hazelcast.jet2.impl.TaskletResult.DONE_WITHOUT_PROGRESS;
import static com.hazelcast.jet2.impl.TaskletResult.MADE_PROGRESS;
import static com.hazelcast.jet2.impl.TaskletResult.NO_PROGRESS;

public class MockQueueHead<T> implements QueueHead<T> {

    private final int chunkSize;
    private final List<T> input;
    private int lastToIndex;
    private boolean paused;

    public MockQueueHead(int chunkSize, List<T> input) {
        this.chunkSize = chunkSize;
        this.input = new ArrayList<>(input);
        this.lastToIndex = 0;
    }

    public void push(T... items) {
        input.addAll(Arrays.asList(items));
    }

    @Override
    public TaskletResult drainTo(Collection<? super T> dest) {
        if (paused) {
            return NO_PROGRESS;
        }
        int from = lastToIndex;
        lastToIndex = Math.min(input.size(), lastToIndex + chunkSize);
        if (from == lastToIndex) {
            return DONE_WITHOUT_PROGRESS;
        }
        for (int i = from; i < lastToIndex; i++) {
            dest.add(input.get(i));
        }
        return MADE_PROGRESS;
    }

    public void pause() {
        paused = true;
    }

    public void resume() {
        paused = false;
    }
}
