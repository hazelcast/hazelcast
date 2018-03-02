/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.util.executor;

import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.util.EmptyStatement.ignore;

public final class PoolExecutorThreadFactory extends AbstractExecutorThreadFactory {

    private final String threadNamePrefix;
    private final AtomicInteger idGen = new AtomicInteger(0);
    // to reuse previous thread IDs
    private final Queue<Integer> idQ = new LinkedBlockingQueue<Integer>(1000);

    public PoolExecutorThreadFactory(String threadNamePrefix, ClassLoader classLoader) {
        super(classLoader);
        this.threadNamePrefix = threadNamePrefix;
    }

    @Override
    protected Thread createThread(Runnable r) {
        Integer id = idQ.poll();
        if (id == null) {
            id = idGen.incrementAndGet();
        }
        String name = threadNamePrefix + id;
        return new ManagedThread(r, name, id);
    }

    private class ManagedThread extends HazelcastManagedThread {

        protected final int id;

        public ManagedThread(Runnable target, String name, int id) {
            super(target, name);
            this.id = id;
        }

        @Override
        protected void afterRun() {
            try {
                idQ.offer(id);
            } catch (Throwable ignored) {
                ignore(ignored);
            }
        }
    }
}
