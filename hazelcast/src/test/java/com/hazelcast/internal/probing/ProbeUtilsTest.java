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

package com.hazelcast.internal.probing;

import static com.hazelcast.internal.probing.ProbeUtils.toLong;
import static com.hazelcast.internal.probing.ProbeUtils.updateInterval;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.junit.Assert.assertEquals;

import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import com.hazelcast.config.WanPublisherState;
import com.hazelcast.internal.util.counters.SwCounter;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.wan.WanSyncStatus;

/**
 * Tests for the {@link ProbeUtils} utility methods.
 */
@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class ProbeUtilsTest {

    @Test
    public void doubleToLongConversion() {
        assertEquals(10000L, ProbeUtils.toLong(1d));
        assertEquals(20000L, ProbeUtils.toLong(2d));
        assertEquals(1000L, ProbeUtils.toLong(0.1d));
        assertEquals(54000L, ProbeUtils.toLong(5.4d));
        assertEquals(12345L, ProbeUtils.toLong(1.2345d));
        assertEquals(12345L, ProbeUtils.toLong(1.23451d));
        assertEquals(12346L, ProbeUtils.toLong(1.23456d));
    }

    @Test
    public void objectToLongConversion() {
        assertEquals(-1L,  toLong(null));
        assertEquals(42L, toLong(new AtomicLong(42L)));
        assertEquals(42L, toLong(new AtomicInteger(42)));
        assertEquals(1L, toLong(new AtomicBoolean(true)));
        assertEquals(0L, toLong(new AtomicBoolean(false)));
        assertEquals(1L, toLong(Boolean.TRUE));
        assertEquals(0L, toLong(Boolean.FALSE));
        assertEquals(42L, toLong(new Long(42L)));
        assertEquals(42L, toLong(new Integer(42)));
        assertEquals(42L, toLong(new Short((short) 42)));
        assertEquals(ProbeUtils.toLong(42d), toLong(new Float(42)));
        assertEquals(ProbeUtils.toLong(42d), toLong(new Double(42)));
        assertEquals(1L, toLong(singletonList("x")));
        assertEquals(1L, toLong(singletonMap("x", "y")));
        assertEquals(42L, toLong(new Semaphore(42)));
        assertEquals(42L, toLong(SwCounter.newSwCounter(42)));
        assertEquals(WanPublisherState.PAUSED.getCode(), toLong(WanPublisherState.PAUSED));
        assertEquals(WanSyncStatus.FAILED.getCode(), toLong(WanSyncStatus.FAILED));
    }

    @Test(expected = UnsupportedOperationException.class)
    public void objectToLongConversion_Unsupported() {
        assertEquals(-1L, toLong(new Character('a')));
    }

    @Test
    public void reprobingCycleTimeToMillis() {
        assertEquals(1000L, updateInterval(1, TimeUnit.SECONDS));
        assertEquals(500L, updateInterval(500, TimeUnit.MILLISECONDS));
    }
}
