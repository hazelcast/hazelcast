/*
 * Copyright (c) 2008-2012, Hazel Bilisim Ltd. All Rights Reserved.
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

package com.hazelcast.partition;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @mdogan 12/3/12
 */
public class PartitionLock {

    private final int sleepTime = 1; // configurable

    private final AtomicBoolean locked = new AtomicBoolean(false);

    private final AtomicInteger readCount = new AtomicInteger();

    public void acquireReadLock(final long time, TimeUnit unit) throws InterruptedException, TimeoutException {
        final long timeInMillis = unit.toMillis(time);
        long elapsed = 0L;
        while (locked.get()) {
            Thread.sleep(sleepTime);
            if ((elapsed += sleepTime) > timeInMillis) {
                throw new TimeoutException();
            }
        }
        readCount.incrementAndGet();
        if (locked.get()) {
            readCount.decrementAndGet();
            acquireReadLock(timeInMillis - elapsed, TimeUnit.MILLISECONDS);
        }
    }

    public void releaseReadLock() {
        readCount.decrementAndGet();
    }

    public void acquireWriteLock() throws InterruptedException {
        while (!locked.compareAndSet(false, true)) {
            Thread.sleep(sleepTime);
        }
        while (readCount.get() > 0) {
            Thread.sleep(sleepTime);
        }
        // go on ...
    }

    public void releaseWriteLock() {
        if (!locked.getAndSet(false)) {
            throw new RuntimeException("!!!!!!!!???????????");
        }
    }

}
