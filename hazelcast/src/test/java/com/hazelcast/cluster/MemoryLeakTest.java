/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.cluster;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.TestUtil;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.NightlyTest;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;

import static java.lang.Thread.sleep;
import static org.junit.Assert.fail;

@RunWith(HazelcastSerialClassRunner.class)
@Category(NightlyTest.class)
public class MemoryLeakTest {

    @BeforeClass
    @AfterClass
    public static void killAllHazelcastInstances() throws IOException {
        Hazelcast.shutdownAll();
    }

    @Test
    public void testShutdownAllMemoryLeak() throws Exception {
        final long usedMemoryInit = getUsedMemoryAsMB();
        for (int i = 0; i < 7; i++) {
            Hazelcast.newHazelcastInstance();
        }
        TestUtil.warmUpPartitions(Hazelcast.getAllHazelcastInstances().toArray(new HazelcastInstance[0]));
        Hazelcast.shutdownAll();
        waitForGC(3 + usedMemoryInit, 60);
    }

    private static long getUsedMemoryAsMB() {
        Runtime.getRuntime().gc();
        long total = Runtime.getRuntime().totalMemory();
        long free = Runtime.getRuntime().freeMemory();
        return (total - free) / 1024 / 1024;
    }

    private static void waitForGC(long limit, int maxSeconds) throws InterruptedException {
        long usedMemoryAsMB;
        for (int i = 0; i < maxSeconds; i++) {
            usedMemoryAsMB = getUsedMemoryAsMB();
            if (usedMemoryAsMB < limit) {
                return;
            }
            sleep(1000);
        }
        fail(String.format("UsedMemory now: %s but expected max: %s", getUsedMemoryAsMB(), limit));
    }
}
