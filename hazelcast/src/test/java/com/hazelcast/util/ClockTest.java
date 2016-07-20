/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.util;

import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertTrue;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class ClockTest extends AbstractClockTest {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @After
    public void tearDown() {
        shutdownIsolatedNode();
        resetClock();
    }

    @Test
    public void testConstructor() {
        assertUtilityConstructor(Clock.class);
    }

    @Test
    public void testCurrentTimeMillis() {
        assertTrue(Clock.currentTimeMillis() > 0);
    }

    @Test
    public void test_whenConfiguringClockOffset_thenSystemOffsetClockIsCreated() {
        System.setProperty(ClockProperties.HAZELCAST_CLOCK_OFFSET, "15");
        startIsolatedNode();

        assertTrue(getClusterTime(isolatedNode) > 0);
    }

    @Test
    public void test_whenConfiguringInvalidClockOffset_thenExceptionIsThrown() {
        System.setProperty(ClockProperties.HAZELCAST_CLOCK_OFFSET, "InvalidNumber");

        expectedException.expectCause(new RootCauseMatcher(NumberFormatException.class));

        startIsolatedNode();
    }

    @Test
    public void test_whenConfiguringNonExistingClockImpl_thenExceptionIsThrown() {
        System.setProperty(ClockProperties.HAZELCAST_CLOCK_IMPL, "NonExistingClockImpl");

        expectedException.expectCause(new RootCauseMatcher(ClassNotFoundException.class));

        startIsolatedNode();
    }
}
