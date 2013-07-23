/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

import java.util.Random;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @mdogan 6/11/13
 */
public final class StripedExecutor implements Executor {

    private final int size;

    private final Executor executor;

    private final Worker[] workers;

    private final Random rand = new Random();

    private volatile boolean live = true;

    public StripedExecutor(Executor executor, int workerCount) {
        size = workerCount;
        this.executor = executor;
        workers = new Worker[workerCount];
        for (int i = 0; i < workerCount; i++) {
            workers[i] = new Worker();
        }
    }

    public void execute(Runnable command) {
        final int key;
        if (command instanceof StripedRunnable) {
            key = ((StripedRunnable) command).getKey();
        } else {
            key = rand.nextInt();
        }
        if (!live) {
            throw new RejectedExecutionException("Executor is terminated!");
        }
        final int index = key != Integer.MIN_VALUE ? Math.abs(key) % size : 0;
        workers[index].execute(command);
    }

    public void shutdown() {
        live = false;
        for (Worker worker : workers) {
            worker.clear();
        }
    }

    private class Worker extends ConcurrentLinkedQueue<Runnable> implements Executor, Runnable {

        private final AtomicBoolean scheduled = new AtomicBoolean(false);

        public void execute(Runnable command) {
            if (!offer(command)) {
                throw new RejectedExecutionException("Worker queue is full!"); // not possible atm.
            }
            schedule();
        }

        private void schedule() {
            if (!isEmpty() && scheduled.compareAndSet(false, true)) {
                try {
                    executor.execute(this);
                } catch (RejectedExecutionException e) {
                    scheduled.set(false);
                    throw e;
                }
            }
        }

        public void run() {
            try {
                Runnable r;
                do {
                    r = poll();
                    if (r != null) {
                        r.run();
                    }
                }
                while (r != null);
            } finally {
                scheduled.set(false);
                schedule();
            }
        }
    }
}
