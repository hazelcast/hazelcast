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

package com.hazelcast.test.starter.constructor.test;

import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.spi.impl.DelegatingCompletableFuture;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.test.starter.constructor.DelegatingCompletableFutureConstructor;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.CompletableFuture;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class DelegatingCompletableFutureConstructorTest {

    @Test
    public void test() {
        CompletableFuture<Integer> delegate = new CompletableFuture<>();
        DelegatingCompletableFuture delegatingCompletableFuture = new DelegatingCompletableFuture(
                new DefaultSerializationServiceBuilder().build(),
                delegate
        );

        DelegatingCompletableFutureConstructor constructor = new DelegatingCompletableFutureConstructor(delegatingCompletableFuture.getClass());

        DelegatingCompletableFuture<Integer> cloned =
                (DelegatingCompletableFuture<Integer>) constructor.createNew(delegatingCompletableFuture);

        delegate.complete(42);
        assertTrue(cloned.isDone());
        assertEquals(42, cloned.join().intValue());
    }
}
