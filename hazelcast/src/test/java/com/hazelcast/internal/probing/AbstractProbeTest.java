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

import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThan;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThat;

import java.util.HashMap;
import java.util.Map;

import org.junit.Before;

import com.hazelcast.internal.metrics.ProbeLevel;
import com.hazelcast.internal.probing.impl.ProbeRegistryImpl;
import com.hazelcast.test.HazelcastTestSupport;

public abstract class AbstractProbeTest extends HazelcastTestSupport {

    protected final ProbeRegistry registry = new ProbeRegistryImpl();
    protected final ProbeRenderContext rendering = registry.newRenderContext(null);
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
        CountingProbeRenderer renderer = probe(expectedKey);
        assertProbedTimes(1, renderer);
        assertNotEquals(-1L, renderer.value);
    }

    protected final void assertProbed(String expectedKey, long expectedValue, double deltaFactor) {
        assertProbed(expectedKey, expectedValue, (long) (expectedValue * deltaFactor));
    }

    protected final void assertProbed(String expectedKey, long expectedValue) {
        assertProbed(expectedKey, expectedValue, 0L);
    }

    protected final void assertProbed(String expectedKey, long expectedValue, long absoluteDelta) {
        CountingProbeRenderer renderer = probe(expectedKey);
        assertProbedTimes(1, renderer);
        assertProbeValue(expectedValue, renderer.value, absoluteDelta);
    }

    protected final void assertNotProbed(String notExpectedKey) {
        assertProbedTimes(0, probe(notExpectedKey));
    }

    protected final void assertProbeCount(int expectedCount) {
        CountingProbeRenderer renderer = probe("");
        assertEquals(expectedCount, renderer.count);
    }

    protected static void assertProbedTimes(int expectedTimes, CountingProbeRenderer actual) {
        String msg = "probe `" + actual.expectedKey + "` occurence ";
        assertEquals(msg, expectedTimes, actual.matches);
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

    CountingProbeRenderer probe(final String expectedKey) {
        CountingProbeRenderer renderer = new CountingProbeRenderer(expectedKey);
        rendering.render(level, renderer);
        return renderer;
    }

    static final class CountingProbeRenderer implements ProbeRenderer {
        final String expectedKey;
        long value = -1;
        int matches = 0;
        int count = 0;
        private CountingProbeRenderer(String expectedKey) {
            this.expectedKey = expectedKey;
        }

        @Override
        public void render(CharSequence key, long value) {
            count++;
            if (expectedKey.contentEquals(key)) {
                matches++;
                this.value = value;
            }
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
