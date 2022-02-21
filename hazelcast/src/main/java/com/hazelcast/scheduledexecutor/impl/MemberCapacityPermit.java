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

package com.hazelcast.scheduledexecutor.impl;

import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

public class MemberCapacityPermit
    implements CapacityPermit {

    private final String name;
    private final int capacity;
    private AtomicInteger permits;

    MemberCapacityPermit(String name, int permits) {
        this.name = name;
        this.capacity = permits;
        this.permits = new AtomicInteger(permits);
    }

    @Override
    public void acquire() throws RejectedExecutionException {
        if (permits.getAndDecrement() <= 0) {
            // Release that incr
            permits.incrementAndGet();

            throw new RejectedExecutionException(
                    "Maximum capacity (" + capacity + ") of tasks reached for this member "
                            + "and scheduled executor (" + name + "). "
                            + "Reminder, that tasks must be disposed if not needed.");

        }

    }

    @Override
    public void acquireQuietly() {
        permits.getAndDecrement();
    }

    @Override
    public void release() {
        permits.incrementAndGet();
    }

    @Override
    public int totalAcquired() {
        return capacity - permits.get();
    }
}
