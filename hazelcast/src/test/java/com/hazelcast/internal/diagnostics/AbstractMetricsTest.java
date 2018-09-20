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

package com.hazelcast.internal.diagnostics;

import static com.hazelcast.internal.probing.CharSequenceUtils.startsWith;
import static java.util.Arrays.asList;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map.Entry;
import java.util.Set;

import com.hazelcast.internal.metrics.ProbeLevel;
import com.hazelcast.internal.probing.ProbeRenderContext;
import com.hazelcast.internal.probing.ProbeSource;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastTestSupport;

public abstract class AbstractMetricsTest extends HazelcastTestSupport {

    protected abstract ProbeRenderContext getRenderContext();

    private ProbeLevel probeLevel = ProbeLevel.INFO;

    public void setProbeLevel(ProbeLevel probeLevel) {
        this.probeLevel = probeLevel;
    }

    protected final void assertHasStatsEventually(int minimumProbes, String type, String name) {
        assertHasStatsEventually(minimumProbes, type, name, "");
    }

    protected final void assertHasStatsEventually(int minimumProbes, String type, String name,
            String additionalPrefix) {
        assertHasStatsEventually(minimumProbes,
                ProbeSource.TAG_TYPE + "=" + type + " "
                        + ProbeSource.TAG_INSTANCE + "=" + name + " "
                        + additionalPrefix);
    }

    protected final void assertHasStatsEventually(final int minimumProbes, final String prefix) {
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertHasStatsWith(minimumProbes, prefix);
            }
        });
    }

    protected final void assertHasStatsWith(int minimumProbes, final String prefix) {
        final StringProbeRenderer renderer = new StringProbeRenderer(prefix);
        getRenderContext().render(probeLevel, renderer);
        assertThat("minimum number of probes ", renderer.probes.size(), greaterThanOrEqualTo(minimumProbes));
        if (minimumProbes > 1) {
            assertHasCreationTime(prefix, renderer);
        }
    }

    private static void assertHasCreationTime(String prefix, StringProbeRenderer renderer) {
        boolean expectCreationTime = prefix.contains(ProbeSource.TAG_INSTANCE + "=")
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

    protected final void assertHasAllStatsEventually(String... expectedKeys) {
        final Set<String> prefixes = new HashSet<String>(asList(expectedKeys));
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                final StringProbeRenderer renderer = new StringProbeRenderer(prefixes);
                getRenderContext().render(probeLevel, renderer);
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
