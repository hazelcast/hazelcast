/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map.impl.proxy;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;

import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class DifferentPutIfAbsentStrategiesTest extends HazelcastTestSupport  {
    @Test
    public void testLatestWriteValue() throws Exception {
        HazelcastInstance hz = createHazelcastInstance();
        IMap<String, String> mapProxy = hz.getMap(randomMapName());
        CountDownLatch locked = new CountDownLatch(1);
        Runnable putWithLock = () -> {
            mapProxy.lock("key");
            try {
                locked.countDown();
                mapProxy.put("key", "put-value");
            } finally {
                mapProxy.unlock("key");
            }
        };

        Callable<String> putIfAbsent = () -> {
            locked.await();
            return mapProxy.putIfAbsent("key", "put-if-absent-value", IMap.ReadPolicy.LATEST_WRITE);
        };

        ExecutorService executor = Executors.newFixedThreadPool(2);
        Future<String> result = executor.submit(putIfAbsent);
        executor.submit(putWithLock);

        assertEquals("put-value", result.get());
    }

    @Test(timeout = 2_000L)
    public void testWeakConsistencyPutIfAbsent() throws Exception {
        HazelcastInstance hz = createHazelcastInstance();
        IMap<String, String> mapProxy = hz.getMap(randomMapName());

        mapProxy.put("key", "prev-value");

        CountDownLatch locked = new CountDownLatch(1);
        CountDownLatch putIfAbsentCompleted = new CountDownLatch(1);
        Runnable putWithLock = () -> {
            mapProxy.lock("key");
            try {
                locked.countDown();
                putIfAbsentCompleted.await();
                mapProxy.put("key", "put-value");
            } catch (InterruptedException e) {
                ignore(e);
            } finally {
                mapProxy.unlock("key");
            }
        };

        Callable<String> putIfAbsent = () -> {
            locked.await();
            String value = mapProxy.putIfAbsent("key", "put-if-absent-value", IMap.ReadPolicy.WEAK);
            putIfAbsentCompleted.countDown();
            return value;
        };

        ExecutorService executor = Executors.newFixedThreadPool(2);
        Future<String> result = executor.submit(putIfAbsent);
        executor.submit(putWithLock);

        assertEquals("prev-value", result.get());
    }
}
