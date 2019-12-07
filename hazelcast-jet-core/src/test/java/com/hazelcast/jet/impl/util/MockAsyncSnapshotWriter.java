/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.impl.util;

import com.hazelcast.internal.serialization.Data;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Map.Entry;

public class MockAsyncSnapshotWriter implements AsyncSnapshotWriter {

    public boolean ableToOffer = true;
    public boolean ableToFlushRemaining = true;
    public boolean hasPendingFlushes;
    public Throwable failure;

    private final Deque<Entry<? extends Data, ? extends Data>> entries = new ArrayDeque<>();
    private boolean isFlushed = true;

    @Override
    public boolean offer(Entry<? extends Data, ? extends Data> entry) {
        if (!ableToOffer) {
            return false;
        }
        entries.add(entry);
        isFlushed = false;
        return true;
    }

    @Override
    public boolean flushAndResetMap() {
        if (ableToFlushRemaining) {
            hasPendingFlushes = !isFlushed;
            isFlushed = true;
        }
        return ableToFlushRemaining;
    }

    @Override
    public void resetStats() {
    }

    @Override
    public boolean hasPendingAsyncOps() {
        return hasPendingFlushes;
    }

    @Override
    public Throwable getError() {
        try {
            return failure;
        } finally {
            failure = null;
        }
    }

    @Override
    public boolean isEmpty() {
        return isFlushed && !hasPendingFlushes;
    }

    public Entry<? extends Data, ? extends Data> poll() {
        return entries.poll();
    }

    @Override
    public long getTotalPayloadBytes() {
        return 0;
    }

    @Override
    public long getTotalKeys() {
        return 0;
    }

    @Override
    public long getTotalChunks() {
        return 0;
    }
}
