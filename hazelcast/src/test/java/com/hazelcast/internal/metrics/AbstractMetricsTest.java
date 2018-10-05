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
    protected final CollectionContext context = registry.openContext();
    private ProbeLevel level = ProbeLevel.DEBUG;

    @Before
    public void setUp() {
        // reset level to debug as default for all tests
        setLevel(ProbeLevel.DEBUG);
    }

    public void setLevel(ProbeLevel level) {
        this.level = level;
    }

    protected final void assertProbed(final String expectedKey) {
        CountingMetricsCollector renderer = runCycle(expectedKey);
        assertProbedTimes(1, renderer);
        assertNotEquals(-1L, renderer.matchValue);
    }

    protected final void assertProbed(String expectedKey, long expectedValue, double deltaFactor) {
        assertProbed(expectedKey, expectedValue, (long) (expectedValue * deltaFactor));
    }

    protected final void assertProbed(String expectedKey, long expectedValue) {
        assertProbed(expectedKey, expectedValue, 0L);
    }

    protected final void assertProbed(String expectedKey, long expectedValue, long absoluteDelta) {
        CountingMetricsCollector renderer = runCycle(expectedKey);
        assertProbedTimes(1, renderer);
        assertProbeValue(expectedValue, renderer.matchValue, absoluteDelta);
    }

    protected final void assertNotProbed(String notExpectedKey) {
        assertProbedTimes(0, runCycle(notExpectedKey));
    }

    protected final void assertProbeCount(int expectedCount) {
        CountingMetricsCollector renderer = runCycle("");
        assertEquals(expectedCount, renderer.totalCount);
    }

    protected static void assertProbedTimes(int expectedTimes, CountingMetricsCollector actual) {
        String msg = "metric `" + actual.expectedKey + "` occurence ";
        assertEquals(msg, expectedTimes, actual.matchCount);
    }

    protected static void assertProbeValue(final long expected, long actual, final long absoluteDelta) {
        if (absoluteDelta == 0L) {
            assertEquals(expected, actual);
        } else {
            assertThat(actual, allOf(
                    greaterThan(expected - absoluteDelta),
                    lessThan(expected + absoluteDelta)));
        }
    }

    CountingMetricsCollector runCycle(final String expectedKey) {
        return runCycle(expectedKey, context, level);
    }

    public static CountingMetricsCollector runCycle(final String expectedKey,
            CollectionContext context, ProbeLevel level) {
        CountingMetricsCollector collector = new CountingMetricsCollector(expectedKey);
        context.collectAll(level, collector);
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
