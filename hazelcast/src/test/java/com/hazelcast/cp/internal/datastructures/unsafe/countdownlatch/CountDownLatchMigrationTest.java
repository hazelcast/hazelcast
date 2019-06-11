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

package com.hazelcast.cp.internal.datastructures.unsafe.countdownlatch;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.cp.ICountDownLatch;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;


@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class CountDownLatchMigrationTest extends HazelcastTestSupport {

    @Test
    public void testLatchMigration() throws InterruptedException {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(5);
        HazelcastInstance hz1 = factory.newHazelcastInstance();
        HazelcastInstance hz2 = factory.newHazelcastInstance();
        warmUpPartitions(hz2, hz1);

        ICountDownLatch latch1 = hz1.getCountDownLatch("test");
        latch1.trySetCount(10);
        Thread.sleep(500);

        ICountDownLatch latch2 = hz2.getCountDownLatch("test");
        assertEquals(10, latch2.getCount());
        latch2.countDown();
        assertEquals(9, latch1.getCount());
        hz1.shutdown();
        assertEquals(9, latch2.getCount());

        HazelcastInstance hz3 = factory.newHazelcastInstance();
        warmUpPartitions(hz3);
        ICountDownLatch latch3 = hz3.getCountDownLatch("test");
        latch3.countDown();
        assertEquals(8, latch3.getCount());

        hz2.shutdown();
        latch3.countDown();
        assertEquals(7, latch3.getCount());

        HazelcastInstance hz4 = factory.newHazelcastInstance();
        HazelcastInstance hz5 = factory.newHazelcastInstance();
        warmUpPartitions(hz5, hz4);
        Thread.sleep(250);

        hz3.shutdown();
        ICountDownLatch latch4 = hz4.getCountDownLatch("test");
        assertEquals(7, latch4.getCount());

        ICountDownLatch latch5 = hz5.getCountDownLatch("test");
        latch5.countDown();
        assertEquals(6, latch5.getCount());
        latch5.countDown();
        assertEquals(5, latch4.getCount());
        assertEquals(5, latch5.getCount());
    }
}
