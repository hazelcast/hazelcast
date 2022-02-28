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

package com.hazelcast.internal.util.executor;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IExecutorService;
import com.hazelcast.spi.impl.DelegatingCompletableFuture;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import java.io.Serializable;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class CancellableDelegatingFutureTest extends HazelcastTestSupport {

    @Rule
    public ExpectedException expected = ExpectedException.none();

    @Test
    public void testInnerFutureThrowsCancellationExceptionWhenOuterFutureIsCancelled() throws Exception {
        final TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(1);
        final HazelcastInstance instance = factory.newHazelcastInstance();
        IExecutorService executorService = instance.getExecutorService(randomString());
        final CompletesOnInterruptionCallable callable = new CompletesOnInterruptionCallable();
        final DelegatingCompletableFuture<Boolean> future = (DelegatingCompletableFuture<Boolean>) executorService.submit(callable);

        if (future.cancel(true)) {
            expected.expect(CancellationException.class);
            future.getDelegate().get();
        }
    }

    @Test
    public void testCancellationOfDoneFutureDoesNothing() throws Exception {
        final TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(1);
        final HazelcastInstance instance = factory.newHazelcastInstance();
        IExecutorService executorService = instance.getExecutorService(randomString());
        final DummyCancellationCallable callable = new DummyCancellationCallable();
        final DelegatingCompletableFuture<Void> future = (DelegatingCompletableFuture<Void>) executorService.submit(callable);

        future.get();
        future.cancel(true);
    }

    static class CompletesOnInterruptionCallable implements Callable<Boolean>, Serializable {

        @Override
        public Boolean call() throws Exception {
            while (true) {
                try {
                    Thread.sleep(1);
                } catch (InterruptedException e) {
                    return Boolean.TRUE;
                }
            }
        }
    }

    static class DummyCancellationCallable implements Callable<Void>, Serializable {
        @Override
        public Void call() throws Exception {
            return null;
        }
    }
}
