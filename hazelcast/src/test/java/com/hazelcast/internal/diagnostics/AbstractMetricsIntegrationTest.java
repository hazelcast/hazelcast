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

import static com.hazelcast.internal.metrics.CharSequenceUtils.startsWith;
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
import com.hazelcast.internal.metrics.CollectionContext;
import com.hazelcast.internal.metrics.MetricsCollector;
import com.hazelcast.internal.metrics.MetricsSource;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastTestSupport;

public abstract class AbstractMetricsIntegrationTest extends HazelcastTestSupport {

    private ProbeLevel probeLevel = ProbeLevel.INFO;
    private CollectionContext context;

    public void setProbeLevel(ProbeLevel probeLevel) {
        this.probeLevel = probeLevel;
    }

    public void setCollectionContext(CollectionContext context) {
        this.context = context;
    }

    protected final void assertHasStatsEventually(int minimumMetrics, String type, String name) {
        assertHasStatsEventually(minimumMetrics, type, name, "");
    }

    protected final void assertHasStatsEventually(int minimumMetrics, String type, String name,
            String additionalPrefix) {
        assertHasStatsEventually(minimumMetrics,
                MetricsSource.TAG_TYPE + "=" + type + " "
                        + MetricsSource.TAG_INSTANCE + "=" + name + " "
                        + additionalPrefix);
    }

    protected final void assertHasStatsEventually(final int minimumMetrics, final String prefix) {
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertHasStatsWith(minimumMetrics, prefix);
            }
        });
    }

    protected final void assertHasStatsWith(int minimumMetrics, final String prefix) {
        final StringMetricsCollector collector = new StringMetricsCollector(prefix);
        context.collectAll(probeLevel, collector);
        assertThat("minimum number of metrics ", collector.matches.size(), greaterThanOrEqualTo(minimumMetrics));
        if (minimumMetrics > 1) {
            assertHasCreationTime(prefix, collector);
        }
    }

    private static void assertHasCreationTime(String prefix, StringMetricsCollector collector) {
        boolean expectCreationTime = prefix.contains(MetricsSource.TAG_INSTANCE + "=")
                && !prefix.contains("type=internal-");
        if (expectCreationTime) {
            for (String key : collector.matches.keySet()) {
                if (key.contains("creationTime")) {
                    return;
                }
            }
            fail("Expected at least one metric with name `creationTime` but found: "
                    + collector.matches.keySet());
        }
    }

    protected final void assertHasAllStatsEventually(String... expectedKeys) {
        final Set<String> prefixes = new HashSet<String>(asList(expectedKeys));
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                final StringMetricsCollector collector = new StringMetricsCollector(prefixes);
                context.collectAll(probeLevel, collector);
                if (!collector.matches.keySet().containsAll(prefixes)) {
                    HashSet<String> missing = new HashSet<String>(prefixes);
                    missing.removeAll(collector.matches.keySet());
                    fail("Missing statistics are: " + missing);
                }
            }
        });
    }

    static class StringMetricsCollector implements MetricsCollector {
        final HashMap<String, Object> matches = new HashMap<String, Object>();
        private final Set<String> expectedPrefixes;

        StringMetricsCollector(String prefix) {
            this(Collections.singleton(prefix));
        }

        StringMetricsCollector(Set<String> expectedPrefixes) {
            this.expectedPrefixes = expectedPrefixes;
        }

        @Override
        public void collect(CharSequence key, long value) {
            if (startsWithAnyPrefix(key)) {
                matches.put(key.toString(), value);
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
            for (Entry<String, Object> match : matches.entrySet()) {
                sb.append(match.getKey()).append(" - ").append(match.getValue()).append("\n");
            }
            return sb.toString();
        }
    }
}
