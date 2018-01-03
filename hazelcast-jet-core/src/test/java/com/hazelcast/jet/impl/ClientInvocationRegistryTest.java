/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.impl;

import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.spi.impl.AbstractCompletableFuture;
import com.hazelcast.test.HazelcastParallelClassRunner;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import java.util.concurrent.Executor;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
public class ClientInvocationRegistryTest {

    @Rule
    public ExpectedException expect = ExpectedException.none();

    @Test
    public void when_duplicateExecutionId_then_exception() {
        ClientInvocationRegistry registry = new ClientInvocationRegistry();
        registry.register(1, new MockFuture<>());

        expect.expect(IllegalStateException.class);
        // duplicate executionId here
        registry.register(1, new MockFuture<>());
    }

    @Test
    public void when_futureCompleted_then_executionIdDeleted() {
        ClientInvocationRegistry registry = new ClientInvocationRegistry();
        MockFuture<Object> f = new MockFuture<>();
        registry.register(1, f);

        f.complete("blabla");

        // duplicate executionId here - should not fail as the former execution was removed
        registry.register(1, new MockFuture<>());
    }

    @Test
    public void testCancel() {
        ClientInvocationRegistry registry = new ClientInvocationRegistry();
        MockFuture<Object> f = new MockFuture<>();
        registry.register(1, f);

        assertFalse(f.isCancelled());

        registry.cancel(1);

        assertTrue(f.isCancelled());
    }

    private static class MockFuture<V> extends AbstractCompletableFuture<V> {

        private ExecutionCallback<V> andThenCallback;

        MockFuture() {
            super((Executor) null, null);
        }

        @Override
        public void andThen(ExecutionCallback<V> callback) {
            this.andThenCallback = callback;
            // do nothing
        }

        public void complete(V response) {
            if (andThenCallback != null) {
                andThenCallback.onResponse(response);
            }
        }

    }

}
