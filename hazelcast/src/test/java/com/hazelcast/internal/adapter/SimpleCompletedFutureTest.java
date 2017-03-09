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

package com.hazelcast.internal.adapter;

import com.hazelcast.core.ExecutionCallback;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class SimpleCompletedFutureTest {

    private SimpleCompletedFuture<String> future;

    @Before
    public void setUp() {
        future = new SimpleCompletedFuture<String>("test");
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testAndThen() {
        future.andThen(new ExecutionCallback<String>() {
            @Override
            public void onResponse(String response) {
            }

            @Override
            public void onFailure(Throwable t) {
            }
        });
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testAndThen_withExecutor() {
        future.andThen(new ExecutionCallback<String>() {
            @Override
            public void onResponse(String response) {
            }

            @Override
            public void onFailure(Throwable t) {
            }
        }, null);
    }

    @Test
    public void testCancel() {
        assertFalse(future.cancel(false));
        assertFalse(future.isCancelled());

        assertFalse(future.cancel(true));
        assertFalse(future.isCancelled());
    }

    @Test
    public void testIsDone() {
        assertTrue(future.isDone());
    }

    @Test
    public void testGet() throws Exception {
        assertEquals("test", future.get());
    }

    @Test
    public void testGet_withTimeout() throws Exception {
        assertEquals("test", future.get(5, TimeUnit.SECONDS));
    }
}
