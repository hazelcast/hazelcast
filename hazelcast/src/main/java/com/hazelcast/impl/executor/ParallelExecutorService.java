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

package com.hazelcast.impl.executor;

import com.hazelcast.logging.ILogger;
import com.hazelcast.util.ConcurrentHashSet;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;

/**
 * todo:
 * the ParallelExecutorService can lead to unbound thread creation.
 */
public class ParallelExecutorService {
    private final ExecutorService executorService;
    private final List<ParallelExecutor> lsParallelExecutors = new CopyOnWriteArrayList<ParallelExecutor>();
    private final ILogger logger;

    public ParallelExecutorService(ILogger logger, ExecutorService executorService) {
        this.executorService = executorService;
        this.logger = logger;
    }

    public void shutdown() {
        for (ParallelExecutor parallelExecutor : lsParallelExecutors) {
            parallelExecutor.shutdown();
        }
        lsParallelExecutors.clear();
    }

    public ParallelExecutor newBlockingParallelExecutor(int concurrencyLevel, int capacity) {
        ParallelExecutor p = new ParallelExecutorImpl(concurrencyLevel, capacity);
        lsParallelExecutors.add(p);
        return p;
    }

    public ParallelExecutor newOrderedBlockingParallelExecutor(int concurrencyLevel, int capacity) {
        ParallelExecutor p = new QueuedExecutorImpl(concurrencyLevel, capacity);
        lsParallelExecutors.add(p);
        return p;
    }



    public ParallelExecutor newParallelExecutor(int concurrencyLevel) {
        ParallelExecutor parallelExecutor;
        //todo: what if concurrencyLevel == 0?
        if (concurrencyLevel > 0 && concurrencyLevel < Integer.MAX_VALUE) {
            parallelExecutor = new ParallelExecutorImpl(concurrencyLevel, Integer.MAX_VALUE);
        } else {
            parallelExecutor = new FullyParallelExecutorImpl();
        }
        lsParallelExecutors.add(parallelExecutor);
        return parallelExecutor;
    }

    //todo: it can happen that a task is sleeping successfully, after the shutdown has been called.
    class FullyParallelExecutorImpl implements ParallelExecutor {

        public void execute(Runnable command) {
            executorService.execute(command);
        }

        public void execute(Runnable command, int hash) {
            executorService.execute(command);
        }

        public void shutdown() {
        }

        public int getPoolSize() {
            return 0;
        }

        public int getActiveCount() {
            return 0;
        }
    }

    private class ParallelExecutorImpl implements ParallelExecutor {
        private final ExecutionSegment[] executionSegments;
        private final AtomicInteger offerIndex = new AtomicInteger();
        private final AtomicInteger activeCount = new AtomicInteger();

        /**
         * Creates a new ParallelExecutorImpl
         *
         * @param concurrencyLevel the concurrency level
         * @param segmentCapacity  the segment capacity. If the segment capacity is Integer.MAX_VALUE, there is
         *                         no bound on the number of tasks that can be stored in the segment. Otherwise
         *                         offering a task to be executed can block until there is capacity to store the
         *                         task.
         */
        private ParallelExecutorImpl(int concurrencyLevel, int segmentCapacity) {
            this.executionSegments = new ExecutionSegment[concurrencyLevel];
            for (int i = 0; i < concurrencyLevel; i++) {
                executionSegments[i] = new ExecutionSegment(segmentCapacity);
            }
        }

        public void execute(Runnable command) {
            int hash = offerIndex.incrementAndGet();
            execute(command, hash);
        }

        public void execute(Runnable command, int hash) {
            if (command == null) {
                throw new NullPointerException("Runnable is not allowed to be null");
            }
            int index = (hash == Integer.MIN_VALUE) ? 0 : Math.abs(hash) % executionSegments.length;
            ExecutionSegment segment = executionSegments[index];
            segment.offer(command);
        }

        public void shutdown() {
            for (ExecutionSegment executionSegment : executionSegments) {
                executionSegment.shutdown();
            }
        }

        public int getPoolSize() {
            int size = 0;
            for (ExecutionSegment executionSegment : executionSegments) {
                size += executionSegment.getPoolSize();
            }
            return size;
        }

        public int getActiveCount() {
            return activeCount.get();
        }

        @SuppressWarnings("SynchronizedMethod")
        private class ExecutionSegment implements Runnable {
            private final BlockingQueue<Runnable> q;
            //this flag helps to guarantee that at most 1 thread at any given moment is running commands from this ExecutionSegment.
            private final AtomicBoolean active = new AtomicBoolean(false);

            private ExecutionSegment(int capacity) {
                q = new LinkedBlockingQueue<Runnable>(capacity);
            }

            private void offer(Runnable command) {
                //put the item on the queue uninterruptibly.
                boolean interrupted = false;
                try {
                    for (; ; ) {
                        try {
                            q.put(command);
                            break;
                        } catch (InterruptedException ie) {
                            interrupted = true;
                        }
                    }
                } finally {
                    if (interrupted) {
                        Thread.currentThread().interrupt();
                    }
                }
                //if the segment is active we don't need to schedule.
                if (active.get()) {
                    return;
                }
                //now we need to do a cas to make sure
                if (active.compareAndSet(false, true)) {
                    executorService.execute(ExecutionSegment.this);
                }
            }

            public void run() {
                activeCount.incrementAndGet();
                try {
                    for (; ; ) {
                        Runnable command = q.poll();
                        if (command == null) {
                            active.set(false);
                            //Here is some complex logic coming: it can happen that work was placed by another thread
                            //after the q.poll. If we don't take care of this situation, it could happen that work remains
                            //unscheduled (and we don't want that).
                            boolean finished;
                            if (q.peek() == null) {
                                //we are lucky, there was no new work scheduled after the ExecutionSegment was made inactive.
                                //It will now be the responsibility of another thread to schedule execution and we can finish.
                                finished = true;
                            } else {
                                //we were unlucky; we decided to deactivate this ExecutionSegment, but new work
                                //was offered. If we can get this ExecutionSegment active again, we keep running, otherwise
                                //it will be the responsibility of another thread to schedule execution and we can finish.
                                //it can be that we are going to continue executing, even though there is no work anymore.
                                //(some other thread could have processed the work that we found with the peek). But that
                                //is not a problem since the g.poll returns null and this thread has the chance to complete
                                //anyway.
                                finished = !active.compareAndSet(false, true);
                            }
                            if (finished) {
                                break;
                            }
                        } else {
                            try {
                                command.run();
                            } catch (Throwable e) {
                                logger.log(Level.WARNING, e.getMessage(), e);
                            }
                        }
                    }
                } finally {
                    activeCount.decrementAndGet();
                }
            }

            private void shutdown() {
                Runnable r = q.poll();
                while (r != null) {
                    r = q.poll();
                }
            }

            private int getPoolSize() {
                return active.get() ? 1 : 0;
            }
        }
    }


    /**
     * This is a simplified form of ThreadPoolExecutor. It does not implement parts of the Executor
     * interface that ParallelExecutor does not such as shutdownNow and core vs pool size.
     *
     * It makes a reasonable effort to ensure that commands are kicked-off in the order they are
     * received by using a single work queue shared by the worker threads. The workers run in threads
     * pulled from the primary hazelcast cached executor.
     */
    private class QueuedExecutorImpl implements ParallelExecutor {

        private final ConcurrentHashSet<Worker> workers;
        private final AtomicInteger workerCount = new AtomicInteger();
        private final AtomicInteger activeCount = new AtomicInteger();
        private LinkedBlockingQueue<Runnable> workQueue;
        private int concurrencyLevel;
        private long keepAliveTime = 5000;

        /**
         * Creates a new ParallelExecutorImpl
         *
         * @param concurrencyLevel the concurrency level
         * @param queueCapacity  the segment capacity. If the segment capacity is Integer.MAX_VALUE, there is
         *                         no bound on the number of tasks that can be stored in the segment. Otherwise
         *                         offering a task to be executed can block until there is capacity to store the
         *                         task.
         */
        private QueuedExecutorImpl(int concurrencyLevel, int queueCapacity) {
            this.concurrencyLevel = concurrencyLevel;
            workQueue = new LinkedBlockingQueue<Runnable>(queueCapacity);

            this.workers = new ConcurrentHashSet<Worker>();
        }

        public void execute(Runnable command) {
            if (command == null) {
                throw new NullPointerException("Runnable is not allowed to be null");
            }
            startThreadIfNeeded();
            offer(command);
            startThreadIfNeeded();

        }

        public void execute(Runnable command, int hash) {
            execute(command);
        }


        private void offer(Runnable command) {
            //put the item on the queue uninterruptibly.
            boolean interrupted = false;
            try {
                for (; ; ) {
                    try {
                        if (workQueue.offer(command, 1 ,TimeUnit.SECONDS))
                            break;
                    } catch (InterruptedException ie) {
                        interrupted = true;
                    }
                }
            } finally {
                if (interrupted) {
                    Thread.currentThread().interrupt();
                }
            }
        }

        public void startThreadIfNeeded() {
            int active;
            if ((active = workerCount.get()) < concurrencyLevel) {

                if (workerCount.compareAndSet(active, active + 1)) {
                    Worker worker = new Worker();
                    workers.add(worker);
                    executorService.submit(worker);
                }
            }
        }


        public void shutdown() {
            for (Worker worker : workers) {
                // check completion
            }
        }

        public int getPoolSize() {
            return workQueue.size();
        }

        public int getActiveCount() {
            return activeCount.get();
        }


        @SuppressWarnings("SynchronizedMethod")
        private class Worker implements Runnable {

            /**
             * Per thread completed task counter; accumulated
             * into completedTaskCount upon termination.
             */
            volatile long completedTasks;

            /**
             * Thread this worker is running in.  Acts as a final field,
             * but cannot be set until thread is created.
             */
            Thread thread;


            private Worker() {
            }

            /**
             * Runs a single task between before/after methods.
             */
            private void runCommand(Runnable command) {
                try {
                    activeCount.incrementAndGet();
                    command.run();
                    completedTasks++;
                } catch (Throwable e) {
                    logger.log(Level.WARNING, e.getMessage(), e);
                } finally {
                    activeCount.decrementAndGet();
                }
            }

            /**
             * Main run loop
             */
            public void run() {
                try {
                    thread = Thread.currentThread();
                    Runnable command = null;
                    while ((command = getTask()) != null) {
                        runCommand(command);
                    }
                } finally {
                    workers.remove(this);
                    workerCount.decrementAndGet();
                }
            }


            Runnable getTask() {
                for (;;) {
                    try {
                        Runnable r;

                        r = workQueue.poll(keepAliveTime, TimeUnit.MILLISECONDS);

                        if (r != null)
                            return r;

                    } catch (InterruptedException ie) {
                        // On interruption, re-check runState
                    }
                }
            }
        }
    }
}
