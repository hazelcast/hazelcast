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

package com.hazelcast.internal.tstore;

import java.util.concurrent.atomic.AtomicLongArray;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;

public final class Epoch {

    private static final AtomicLongFieldUpdater<Epoch> CURRENT = AtomicLongFieldUpdater.newUpdater(Epoch.class, "current");

    private volatile long current = 1;
    private volatile long safe;

    private final AtomicLongArray threadEpochs;

    public Epoch(int maxThreads) {
        this.threadEpochs = new AtomicLongArray(maxThreads);
    }

    public void acquire(int threadIndex) {
        // TODO
        threadEpochs.set(threadIndex, current);
    }

    public void refresh(int threadIndex) {
        // TODO
    }

    public void bump(int threadIndex, Runnable action) {
        // TODO
    }

    public void release(int threadIndex) {
        // TODO
    }

}
