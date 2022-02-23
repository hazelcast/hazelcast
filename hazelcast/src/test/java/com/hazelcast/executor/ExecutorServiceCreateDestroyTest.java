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

package com.hazelcast.executor;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IExecutorService;
import com.hazelcast.cluster.Member;
import com.hazelcast.spi.exception.DistributedObjectDestroyedException;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ExecutorServiceCreateDestroyTest extends HazelcastTestSupport {

    private static final int INSTANCE_COUNT = 3;

    private TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory();
    private HazelcastInstance[] instances = new HazelcastInstance[INSTANCE_COUNT];

    @Before
    public void setup() {
        for (int i = 0; i < instances.length; i++) {
            instances[i] = factory.newHazelcastInstance();
        }
        warmUpPartitions(instances);
    }

    @Test
    public void test_createSubmit_thenDestroy() throws Exception {
        test_createUse_thenDestroy(new ExecutorServiceCommand() {
            @Override
            <T> Collection<Future<T>> submit(IExecutorService ex, Callable<T> task) {
                return Collections.singleton(ex.submit(task));
            }
        });
    }

    @Test
    public void test_createSubmitAllMembers_thenDestroy() throws Exception {
        test_createUse_thenDestroy(new ExecutorServiceCommand() {
            @Override
            <T> Collection<Future<T>> submit(IExecutorService ex, Callable<T> task) {
                Map<Member, Future<T>> futures = ex.submitToAllMembers(task);
                return futures.values();
            }
        });
    }

    private void test_createUse_thenDestroy(final ExecutorServiceCommand command) throws Exception {
        Future[] futures = new Future[INSTANCE_COUNT];
        for (int i = 0; i < INSTANCE_COUNT; i++) {
            final HazelcastInstance instance = instances[i];

            futures[i] = spawn(() -> {
                Random rand = new Random();
                for (int i1 = 0; i1 < 1000; i1++) {
                    LockSupport.parkNanos(1 + rand.nextInt(100));
                    IExecutorService ex = instance.getExecutorService("executor");
                    command.run(ex);
                    ex.destroy();
                }
                return null;
            });
        }

        for (Future future : futures) {
            try {
                future.get(ASSERT_TRUE_EVENTUALLY_TIMEOUT, TimeUnit.SECONDS);
            } catch (ExecutionException e) {
                // DistributedObjectDestroyedException is ignored, it may be thrown
                // when executor is destroyed as it is being created from another thread
                if (!(e.getCause() instanceof DistributedObjectDestroyedException)) {
                    throw e;
                }
            }
        }
    }

    private abstract static class ExecutorServiceCommand {
        final void run(IExecutorService ex) throws Exception {
            try {
                Collection<Future<Void>> futures = submit(ex, new VoidCallableTask());
                for (Future<Void> future : futures) {
                    future.get();
                }
            } catch (RejectedExecutionException ignored) {
            } catch (ExecutionException e) {
                // This looks like an unexpected behaviour!
                // When a task is rejected, it's wrapped in ExecutionException.
                if (!(e.getCause() instanceof RejectedExecutionException)) {
                    throw e;
                }
            }
        }

        abstract <T> Collection<Future<T>> submit(IExecutorService ex, Callable<T> task);
    }

    private static class VoidCallableTask implements Callable<Void>, Serializable {
        @Override
        public Void call() throws Exception {
            return null;
        }
    }
}
