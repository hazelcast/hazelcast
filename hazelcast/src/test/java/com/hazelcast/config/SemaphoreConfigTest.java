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

package com.hazelcast.config;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import nl.jqno.equalsverifier.EqualsVerifier;
import nl.jqno.equalsverifier.Warning;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.test.HazelcastTestSupport.assumeDifferentHashCodes;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
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

    @Test
    public void testEqualsAndHashCode() {
        assumeDifferentHashCodes();
        EqualsVerifier.forClass(SemaphoreConfig.class)
                .allFieldsShouldBeUsedExcept("readOnly")
                .suppress(Warning.NONFINAL_FIELDS)
                .withPrefabValues(SemaphoreConfigReadOnly.class,
                        new SemaphoreConfigReadOnly(new SemaphoreConfig().setName("red")),
                        new SemaphoreConfigReadOnly(new SemaphoreConfig().setName("black")))
                .verify();
    }
}
