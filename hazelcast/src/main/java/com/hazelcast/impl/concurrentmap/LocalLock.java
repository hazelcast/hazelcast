/* 
 * Copyright (c) 2008-2010, Hazel Ltd. All Rights Reserved.
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
 *
 */

package com.hazelcast.impl.concurrentmap;

public class LocalLock {
    final int threadId;
    volatile int count = 0;

    public LocalLock(int threadId) {
        this.threadId = threadId;
    }

    public int getThreadId() {
        return threadId;
    }

    public int getCount() {
        return count;
    }

    public int incrementAndGet() {
        return ++count;
    }

    public int decrementAndGet() {
        return --count;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        LocalLock localLock = (LocalLock) o;
        if (threadId != localLock.threadId) return false;
        return true;
    }

    @Override
    public int hashCode() {
        return threadId;
    }

    @Override
    public String toString() {
        return "LocalLock{" +
                "threadId=" + threadId +
                ", count=" + count +
                '}';
    }
}
