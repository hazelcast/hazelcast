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

package com.hazelcast.internal.longregister.client;

import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.cp.IAtomicLong;
import com.hazelcast.internal.longregister.LongRegisterService;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.CompletableFuture;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClientLongRegisterTest extends HazelcastTestSupport {

    private final TestHazelcastFactory hazelcastFactory = new TestHazelcastFactory();
    private IAtomicLong longRegister;

    @Before
    public void setup() {
        hazelcastFactory.newHazelcastInstance();
        HazelcastInstance client = hazelcastFactory.newHazelcastClient();
        longRegister = client.getDistributedObject(LongRegisterService.SERVICE_NAME, randomString());
    }

    @After
    public void tearDown() {
        hazelcastFactory.terminateAll();
    }

    @Test
    public void test() throws Exception {
        assertEquals(0, longRegister.getAndAdd(2));
        assertEquals(2, longRegister.get());
        longRegister.set(5);
        assertEquals(5, longRegister.get());
        assertEquals(8, longRegister.addAndGet(3));
        assertEquals(8, longRegister.get());
        assertEquals(7, longRegister.decrementAndGet());
        assertEquals(7, longRegister.getAndIncrement());
        assertEquals(8, longRegister.getAndSet(9));
        assertEquals(9, longRegister.getAndDecrement());
        assertEquals(9, longRegister.incrementAndGet());
    }

    @Test
    public void testAsync() throws Exception {
        CompletableFuture<Long> future = longRegister.getAndAddAsync(10).toCompletableFuture();
        assertEquals(0, future.get().longValue());

        future = longRegister.getAsync().toCompletableFuture();
        assertEquals(10, future.get().longValue());

        future = longRegister.incrementAndGetAsync().toCompletableFuture();
        assertEquals(11, future.get().longValue());

        future = longRegister.addAndGetAsync(-13).toCompletableFuture();
        assertEquals(-2, future.get().longValue());
    }

}
