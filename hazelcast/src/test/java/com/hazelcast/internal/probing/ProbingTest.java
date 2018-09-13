package com.hazelcast.internal.probing;

import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThan;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThat;

import java.util.HashMap;
import java.util.Map;

import com.hazelcast.internal.metrics.ProbeLevel;
import com.hazelcast.internal.probing.ProbeRegistry.ProbeRenderContext;
import com.hazelcast.test.HazelcastTestSupport;

public abstract class ProbingTest extends HazelcastTestSupport {

    final ProbeRegistry registry = new ProbeRegistryImpl();
    final ProbeRenderContext rendering = registry.newRenderingContext();

    final void assertProbed(final String expectedKey) {
        CountingProbeRenderer renderer = probe(expectedKey);
        assertProbedTimes(1, renderer);
        assertNotEquals(-1L, renderer.value);
    }

    final void assertProbed(final String expectedKey, long expectedValue, 
            double deltaFactor) {
        assertProbed(expectedKey, expectedValue, (long)(expectedValue * deltaFactor));
    }

    final void assertProbed(final String expectedKey, final long expectedValue) {
        assertProbed(expectedKey, expectedValue, 0L);
    }

    final void assertProbed(final String expectedKey, final long expectedValue,
            final long absoluteDelta) {
        CountingProbeRenderer renderer = probe(expectedKey);
        assertProbedTimes(1, renderer);
        assertProbeValue(expectedValue, renderer.value, absoluteDelta);
    }

    final void assertNotProbed(String notExpectedKey) {
        assertProbedTimes(0, probe(notExpectedKey));
    }

    static void assertProbedTimes(int expectedTimes, CountingProbeRenderer actual) {
        assertEquals("probe `" + actual.expectedKey + "` occurence ", expectedTimes, actual.matches);
    }

    static void assertProbeValue(final long expected, long actual, final long absoluteDelta) {
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
        rendering.renderAt(ProbeLevel.DEBUG, renderer);
        return renderer;
    }

    static final class CountingProbeRenderer implements ProbeRenderer {
        final String expectedKey;
        long value = -1;
        int matches = 0;

        private CountingProbeRenderer(String expectedKey) {
            this.expectedKey = expectedKey;
        }

        @Override
        public void render(CharSequence key, long value) {
            if (expectedKey.contentEquals(key)) {
                matches++;
                this.value = value;
            }
        }
    }

    static Map<Integer, Integer> createMap(int size) {
        Map<Integer, Integer> map = new HashMap<Integer, Integer>();
        for (int k = 0; k < size; k++) {
            map.put(k, k);
        }
        return map;
    }
}
