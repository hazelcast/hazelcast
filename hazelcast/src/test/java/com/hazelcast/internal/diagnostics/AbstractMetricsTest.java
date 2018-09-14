package com.hazelcast.internal.diagnostics;

import static com.hazelcast.internal.probing.Probing.startsWith;
import static java.util.Arrays.asList;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map.Entry;
import java.util.Set;

import org.junit.Before;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.metrics.ProbeLevel;
import com.hazelcast.internal.probing.ProbeRegistry;
import com.hazelcast.internal.probing.ProbeRegistry.ProbeRenderContext;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastTestSupport;

public abstract class AbstractMetricsTest extends HazelcastTestSupport {

    protected HazelcastInstance hz;
    private ProbeRegistry registry;
    private ProbeRenderContext renderContext;

    abstract Config configure();

    @Before
    public void setup() {
        hz = createHazelcastInstance(configure());
        registry = getNode(hz).nodeEngine.getProbeRegistry();
        renderContext = registry.newRenderingContext();
        warmUpPartitions(hz);
    }

    final void assertHasStatsEventually(int minimumProbes, String type, String name) {
        assertHasStatsEventually(minimumProbes, type, name, "");
    }

    final void assertHasStatsEventually(int minimumProbes, String type, String name,
            String additionalPrefix) {
        assertHasStatsEventually(minimumProbes,
                  ProbeRegistry.ProbeSource.TAG_TYPE + "=" + type + " "
                + ProbeRegistry.ProbeSource.TAG_INSTANCE + "=" + name + " "
                + additionalPrefix);
    }

    final void assertHasStatsEventually(final int minimumProbes, final String prefix) {
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertHasStatsWith(minimumProbes, prefix);
            }
        });
    }

    final void assertHasStatsWith(int minimumProbes, final String prefix) {
        final StringProbeRenderer renderer = new StringProbeRenderer(prefix);
        renderContext.renderAt(ProbeLevel.INFO, renderer);
        assertThat("minimum number of probes ", renderer.probes.size(), greaterThanOrEqualTo(minimumProbes));
        if (minimumProbes > 0) {
            assertHasCreationTime(prefix, renderer);
        }
    }

    private static void assertHasCreationTime(String prefix, StringProbeRenderer renderer) {
        boolean expectCreationTime = prefix.contains(ProbeRegistry.ProbeSource.TAG_INSTANCE + "=")
                && !prefix.contains("type=internal-");
        if (expectCreationTime) {
            for (String key : renderer.probes.keySet()) {
                if (key.contains("creationTime")) {
                    return;
                }
            }
            fail("Expected at least one metric with name `creationTime` but found: "
                    + renderer.probes.keySet());
        }
    }

    final void assertHasAllStatsEventually(String... expectedKeys) {
        final Set<String> prefixes = new HashSet<String>(asList(expectedKeys));
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                final StringProbeRenderer renderer = new StringProbeRenderer(prefixes);
                renderContext.renderAt(ProbeLevel.INFO, renderer);
                if (!renderer.probes.keySet().containsAll(prefixes)) {
                    HashSet<String> missing = new HashSet<String>(prefixes);
                    missing.removeAll(renderer.probes.keySet());
                    fail("Missing statistics are: " + missing);
                }
            }
        });
    }

    static class StringProbeRenderer implements com.hazelcast.internal.probing.ProbeRenderer {
        final HashMap<String, Object> probes = new HashMap<String, Object>();
        private final Set<String> expectedPrefixes;

        StringProbeRenderer(String prefix) {
            this(Collections.singleton(prefix));
        }

        StringProbeRenderer(Set<String> expectedPrefixes) {
            this.expectedPrefixes = expectedPrefixes;
        }

        @Override
        public void render(CharSequence key, long value) {
            if (startsWithAnyPrefix(key)) {
                probes.put(key.toString(), value);
            }
        }

        private boolean startsWithAnyPrefix(CharSequence key) {
            for (String prefix : expectedPrefixes) {
                if (startsWith(prefix, key)) {
                    return true;
                }
            }
            return false;
        }

        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder();
            for (Entry<String, Object> probe : probes.entrySet()) {
                sb.append(probe.getKey()).append(" - ").append(probe.getValue()).append("\n");
            }
            return sb.toString();
        }
    }
}
