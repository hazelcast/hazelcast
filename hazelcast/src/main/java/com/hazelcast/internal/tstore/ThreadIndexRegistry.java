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

import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

/**
 * Assigns an unique integer ID (thread index) to every participating
 * thread.
 */
public final class ThreadIndexRegistry {

    private static final AtomicIntegerFieldUpdater<ThreadIndexRegistry> COUNT =
            AtomicIntegerFieldUpdater.newUpdater(ThreadIndexRegistry.class, "count");

    private final int maxThreads;
    private volatile int count;

    public ThreadIndexRegistry(int maxThreads) {
        this.maxThreads = maxThreads;
    }

    public int generateThreadIndex() {
        int index = COUNT.getAndIncrement(this);
        if (index >= maxThreads) {
            throw new RuntimeException("thread index registry overflow");
        }
        return index;
    }

}
