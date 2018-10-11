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

package com.hazelcast.internal.metrics;

import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThan;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThat;

import java.util.HashMap;
import java.util.Map;

import org.junit.Before;

import com.hazelcast.internal.metrics.impl.MetricsRegistryImpl;
import com.hazelcast.test.HazelcastTestSupport;

public abstract class AbstractMetricsTest extends HazelcastTestSupport {

    protected final MetricsRegistry registry = new MetricsRegistryImpl();
    protected CollectionContext context = registry.openContext(ProbeLevel.DEBUG);

    @Before
    public void setUp() {
        // reset level to debug as default for all tests
        setLevel(ProbeLevel.DEBUG);
    }

    public void setLevel(ProbeLevel level) {
        this.context = registry.openContext(level);
    }

    protected final void assertCollected(final String expectedKey) {
        CountingMetricsCollector collector = runCycle(expectedKey);
        assertCollectedTimes(1, collector);
        assertNotEquals(-1L, collector.matchValue);
    }

    protected final void assertCollected(String expectedKey, long expectedValue, double deltaFactor) {
        assertCollected(expectedKey, expectedValue, (long) (expectedValue * deltaFactor));
    }

    protected final void assertCollected(String expectedKey, long expectedValue) {
        assertCollected(expectedKey, expectedValue, 0L);
    }

    protected final void assertCollected(String expectedKey, long expectedValue, long absoluteDelta) {
        CountingMetricsCollector collector = runCycle(expectedKey);
        assertCollectedTimes(1, collector);
        assertCollectedValue(expectedValue, collector.matchValue, absoluteDelta);
    }

    protected final void assertNotCollected(String notExpectedKey) {
        assertCollectedTimes(0, runCycle(notExpectedKey));
    }

    protected final void assertCollectedCount(int expectedCount) {
        CountingMetricsCollector collector = runCycle("");
        assertEquals(expectedCount, collector.totalCount);
    }

    protected static void assertCollectedTimes(int expectedTimes, CountingMetricsCollector actual) {
        String msg = "metric `" + actual.expectedKey + "` occurence ";
        assertEquals(msg, expectedTimes, actual.matchCount);
    }

    protected static void assertCollectedValue(final long expected, long actual, final long absoluteDelta) {
        if (absoluteDelta == 0L) {
            assertEquals(expected, actual);
        } else {
            assertThat(actual, allOf(
                    greaterThan(expected - absoluteDelta),
                    lessThan(expected + absoluteDelta)));
        }
    }

    CountingMetricsCollector runCycle(final String expectedKey) {
        return runCycle(expectedKey, context);
    }

    public static CountingMetricsCollector runCycle(final String expectedKey,
            CollectionContext context) {
        CountingMetricsCollector collector = new CountingMetricsCollector(expectedKey);
        context.collectAll(collector);
        return collector;
    }

    public static final class CountingMetricsCollector implements MetricsCollector {
        final String expectedKey;
        long matchValue = -1;
        int matchCount = 0;
        int totalCount = 0;
        private CountingMetricsCollector(String expectedKey) {
            this.expectedKey = expectedKey;
        }

        @Override
        public void collect(CharSequence key, long value) {
            totalCount++;
            if (expectedKey.contentEquals(key)) {
                matchCount++;
                this.matchValue = value;
            }
        }

        public long getMatchValue() {
            return matchValue;
        }

        public int getMatchCount() {
            return matchCount;
        }

        public int getTotalCount() {
            return totalCount;
        }
    }

    protected static Map<String, Integer> createMap(int size) {
        Map<String, Integer> map = new HashMap<String, Integer>();
        for (int k = 0; k < size; k++) {
            map.put(String.valueOf(k), k);
        }
        return map;
    }
}
