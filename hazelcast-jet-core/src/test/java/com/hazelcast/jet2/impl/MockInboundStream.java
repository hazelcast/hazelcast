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

public class MockInboundStream implements InboundEdgeStream {

    private final int chunkSize;
    private final List<Object> input;
    private int inputIndex;
    private boolean paused;
    private boolean done;

    public MockInboundStream(int chunkSize, List<?> input) {
        this.chunkSize = chunkSize;
        this.input = new ArrayList<>(input);
        this.inputIndex = 0;
    }

    public void push(Object... items) {
        input.addAll(Arrays.asList(items));
    }

    @Override
    public TaskletResult drainTo(CollectionWithObserver dest) {
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
        if (inputIndex == input.size()) {
            done = true;
            return DONE;
        } else {
            return MADE_PROGRESS;
        }
    }

    @Override
    public boolean isDone() {
        return done;
    }

    @Override
    public int ordinal() {
        return 0;
    }

    @Override
    public int priority() {
        return 0;
    }

    public void pause() {
        paused = true;
    }

    public void resume() {
        paused = false;
    }
}
