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

package com.hazelcast.spi.impl.operationservice.impl;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.spi.InternalCompletableFuture;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.impl.operationservice.InternalOperationService;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import java.util.concurrent.CancellationException;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class InvocationFuture_CancelTest extends HazelcastTestSupport {

    private static final int RESULT = 123;

    @Rule
    public final ExpectedException exceptionRule = ExpectedException.none();

    private InternalOperationService opService;

    @Before
    public void setup() {
        HazelcastInstance hz = createHazelcastInstance();
        opService = getOperationService(hz);
    }

    @Test
    public void whenCallCancel_thenCancelled() throws Exception {
        // Given
        ICompletableFuture future = invoke();

        // When
        boolean result = future.cancel(true);

        // Then
        assertTrue(result);
        assertTrue(future.isCancelled());
        assertTrue(future.isDone());
    }

    @Test
    public void whenCancelled_thenCantCancelAgain() throws Exception {
        // Given
        ICompletableFuture future = invoke();

        // When
        future.cancel(true);

        // Then
        assertFalse(future.cancel(true));
    }

    @Test
    public void whenCancelled_thenGetThrowsCancelled() throws Exception {
        // Given
        ICompletableFuture future = invoke();

        // When
        future.cancel(true);

        // Then
        exceptionRule.expect(CancellationException.class);
        future.get();
    }

    private InternalCompletableFuture invoke() {
        Operation op = new Operation() {
            @Override
            public void run() throws Exception {
                sleepMillis(1000);
            }

            @Override
            public Object getResponse() {
                return RESULT;
            }
        };
        return opService.invokeOnPartition(null, op, 0);
    }
}
