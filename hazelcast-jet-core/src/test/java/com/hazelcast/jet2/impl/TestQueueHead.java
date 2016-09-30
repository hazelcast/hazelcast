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
import java.util.List;

public class TestQueueHead<T> implements QueueHead<T> {

    private final int chunkSize;
    private final List<T> input;
    private int lastToIndex;
    private boolean paused;

    public TestQueueHead(int chunkSize, List<T> input) {
        this.chunkSize = chunkSize;
        this.input = new ArrayList<>(input);
        this.lastToIndex = 0;
    }

    public void push(T... items) {
        input.addAll(Arrays.asList(items));
    }

    @Override
    public Chunk<T> pollChunk() {
        int from = lastToIndex;
        lastToIndex = Math.min(input.size(), lastToIndex + chunkSize);

        if (paused) {
            return new ListChunk<>(new ArrayList<>());
        }

        if (from == lastToIndex) {
            return null;
        }

        List<T> chunk = new ArrayList<>();
        for (int i = from; i < lastToIndex; i++) {
            chunk.add(input.get(i));
        }
        return new ListChunk<>(chunk);
    }

    public void pause() {
        paused = true;
    }

    public void resume() {
        paused = false;
    }
}
