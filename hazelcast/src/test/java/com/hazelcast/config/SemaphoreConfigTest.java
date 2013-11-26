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
package com.hazelcast.config;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class SemaphoreConfigTest {

    @Test
    public void testSetInitialPermits() {
        SemaphoreConfig semaphoreConfig = new SemaphoreConfig().setInitialPermits(1234);
        assertTrue(semaphoreConfig.getInitialPermits() == 1234);
    }

    @Test
    public void shouldAcceptZeroInitialPermits() {
        SemaphoreConfig semaphoreConfig = new SemaphoreConfig().setInitialPermits(0);
        assertTrue(semaphoreConfig.getInitialPermits() == 0);
    }

    @Test
    public void shouldAcceptNegativeInitialPermits() {
        SemaphoreConfig semaphoreConfig = new SemaphoreConfig().setInitialPermits(-1234);
        assertTrue(semaphoreConfig.getInitialPermits() == -1234);
    }
}
