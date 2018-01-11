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

package com.hazelcast.mapreduce.impl.task;

import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class ReducerTaskSchedulerTest extends HazelcastTestSupport {

    private ExecutorService executorService;
    private Semaphore semaphore;
    private TaskWithSemaphore task;

    private ReducerTaskScheduler scheduler;

    @Before
    public void setUp() {
        executorService = Executors.newFixedThreadPool(3);
        semaphore = new Semaphore(0);
        task = new TaskWithSemaphore(semaphore);
        scheduler = new ReducerTaskScheduler(executorService, task);
    }

    @After
    public void tearDown() {
        executorService.shutdown();
    }

    @Test
    public void requestExecution_whenInActive_thenExecuteTaskEventually() {
        scheduler.requestExecution();
        semaphore.release();

        assertTrueEventually(
                new AssertTask() {
                    @Override
                    public void run() throws Exception {
                        assertEquals(1, task.getExecutionCount());
                    }
                }
        );
    }

    @Test
    public void requestExecution_whenTriggeredWhileOtherTaskIsStillRunning_thenExecuteTaskTwice() {
        scheduler.requestExecution();
        scheduler.requestExecution();
        semaphore.release();
        semaphore.release();

        assertTrueEventually(
                new AssertTask() {
                    @Override
                    public void run() throws Exception {
                        assertEquals(2, task.getExecutionCount());
                    }
                }
        );
    }

    @Test
    public void requestExecution_whenTriggeredWhileAlreadyRequested_thenExecuteTaskTwice() {
        scheduler.requestExecution();
        scheduler.requestExecution();
        scheduler.requestExecution();
        semaphore.release();
        semaphore.release();
        semaphore.release();

        assertTrueEventually(
                new AssertTask() {
                    @Override
                    public void run() throws Exception {
                        assertEquals(2, task.getExecutionCount());
                    }
                }
        );
    }


    private class TaskWithSemaphore implements Runnable {
        private Semaphore semaphore;
        private AtomicInteger executionCount = new AtomicInteger();

        private TaskWithSemaphore(Semaphore semaphore) {
            this.semaphore = semaphore;
        }

        public int getExecutionCount() {
            return executionCount.get();
        }

        @Override
        public void run() {
            executionCount.incrementAndGet();
            try {
                semaphore.acquire();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            scheduler.afterExecution();
        }
    }

}
