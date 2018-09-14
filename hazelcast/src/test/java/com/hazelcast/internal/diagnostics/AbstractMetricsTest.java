package com.hazelcast.internal.diagnostics;

import static com.hazelcast.internal.probing.Probing.startsWith;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import java.util.HashMap;
import java.util.Map.Entry;

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
        if (minimumProbes > 0 && prefix.contains(ProbeRegistry.ProbeSource.TAG_INSTANCE + "=")) {
            for (String key : renderer.probes.keySet()) {
                if (key.contains("creationTime")) {
                    return;
                }
            }
            fail("Expected at least one metric with name `creationTime` but found: "
                    + renderer.probes.keySet());
        }
    }

    static class StringProbeRenderer implements com.hazelcast.internal.probing.ProbeRenderer {
        final HashMap<String, Object> probes = new HashMap<String, Object>();
        private final String prefix;

        StringProbeRenderer(String prefix) {
            this.prefix = prefix;
        }

        @Override
        public void render(CharSequence key, long value) {
            if (startsWith(prefix, key)) {
                probes.put(key.toString(), value);
            }
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
