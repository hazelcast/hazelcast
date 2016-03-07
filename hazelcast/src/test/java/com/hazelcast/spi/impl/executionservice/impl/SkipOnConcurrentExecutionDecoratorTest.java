/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.spi.impl.executionservice.impl;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class SkipOnConcurrentExecutionDecoratorTest extends HazelcastTestSupport {

    @Test
    public void givenTheTaskIsNotRunning_whenThreadAttemptToExecuteIt_theTaskWillBeExecuted() throws InterruptedException {
        ResumableCountingRunnable task = new ResumableCountingRunnable();
        decorateAndStartOnDifferentThread(task);

        task.awaitExecutionStarted();
        task.resumeExecution();

        assertEquals(1, task.getExecutionCount());
    }


    @Test
    public void givenTheTaskIsAlreadyRunning_whenThreadAttemptToExecuteIt_theExutionWillBeSkipped() throws InterruptedException {
        final ResumableCountingRunnable task = new ResumableCountingRunnable();

        //start first task
        SkipOnConcurrentExecutionDecorator decoratedTask = decorateAndStartOnDifferentThread(task);

        //wait until the task is running
        task.awaitExecutionStarted();

        //attempt to start execution from the test thread. this execution should be skipped -> it won't block
        decoratedTask.run();

        //resume the original task
        task.resumeExecution();

        assertEquals(1, task.getExecutionCount());
    }


    private SkipOnConcurrentExecutionDecorator decorateAndStartOnDifferentThread(Runnable task) {
        SkipOnConcurrentExecutionDecorator decoratedTask = new SkipOnConcurrentExecutionDecorator(task);
        new Thread(decoratedTask).start();
        return decoratedTask;
    }

    private class ResumableCountingRunnable implements Runnable {
        private final AtomicInteger executionCount = new AtomicInteger();
        private final Semaphore resumeSemaphore = new Semaphore(0);
        private final Semaphore startedSemaphore = new Semaphore(0);


        @Override
        public void run() {
            executionCount.incrementAndGet();
            startedSemaphore.release();
            try {
                resumeSemaphore.acquire();
            } catch (InterruptedException e) {
                fail("Thread interrupted while waiting on latch");
            }
        }

        public void awaitExecutionStarted() throws InterruptedException {
            startedSemaphore.acquire();
        }

        public void resumeExecution() {
            resumeSemaphore.release();
        }

        public int getExecutionCount() {
            return executionCount.get();
        }
    }
}
