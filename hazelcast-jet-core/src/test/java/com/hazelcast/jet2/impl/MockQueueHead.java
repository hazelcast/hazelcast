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
import java.util.Collection;
import java.util.List;

import static com.hazelcast.jet2.impl.TaskletResult.DONE;
import static com.hazelcast.jet2.impl.TaskletResult.MADE_PROGRESS;
import static com.hazelcast.jet2.impl.TaskletResult.NO_PROGRESS;
import static com.hazelcast.jet2.impl.TaskletResult.WAS_ALREADY_DONE;

public class MockQueueHead<T> implements QueueHead<T> {

    private final int chunkSize;
    private final List<T> input;
    private int inputIndex;
    private boolean paused;

    public MockQueueHead(int chunkSize, List<T> input) {
        this.chunkSize = chunkSize;
        this.input = new ArrayList<>(input);
        this.inputIndex = 0;
    }

    public void push(T... items) {
        input.addAll(Arrays.asList(items));
    }

    @Override
    public TaskletResult drainTo(Collection<? super T> dest) {
        if (paused) {
            return NO_PROGRESS;
        }
        final int limit = Math.min(input.size(), inputIndex + chunkSize);
        if (limit == inputIndex) {
            return WAS_ALREADY_DONE;
        }
        for (; inputIndex < limit; inputIndex++) {
            dest.add(input.get(inputIndex));
        }
        return inputIndex == input.size() ? DONE : MADE_PROGRESS;
    }

    public void pause() {
        paused = true;
    }

    public void resume() {
        paused = false;
    }
}
