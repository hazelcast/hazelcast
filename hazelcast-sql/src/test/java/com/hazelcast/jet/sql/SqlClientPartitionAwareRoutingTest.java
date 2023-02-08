/*
 * Copyright 2023 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.jet.sql;

import com.hazelcast.cluster.Member;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.metrics.MetricDescriptor;
import com.hazelcast.internal.metrics.collectors.MetricsCollector;
import com.hazelcast.jet.core.metrics.MetricTags;
import com.hazelcast.partition.Partition;
import com.hazelcast.sql.metrics.MetricNames;
import com.hazelcast.test.Accessors;
import com.hazelcast.test.HazelcastSerialClassRunner;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

@RunWith(HazelcastSerialClassRunner.class)
public class SqlClientPartitionAwareRoutingTest extends SqlTestSupport {
    @BeforeClass
    public static void beforeClass() {
        initializeWithClient(3, null, null);
    }

    @Before
    public void init() {
        client().getSql().execute("DROP MAPPING IF EXISTS test");
        createMapping("test", Integer.class, String.class);
    }

    @Test
    public void test_selectRouting() {
        testQuery("SELECT * FROM test WHERE __key = ?", MetricNames.FAST_QUERIES_EXECUTED, 100);
    }

    @Test
    public void test_insertRouting() {
        testQuery("INSERT INTO test (this, __key) VALUES ('testVal', ?)", MetricNames.FAST_QUERIES_EXECUTED, 100);
    }

    @Test
    public void test_updateRouting() {
        testQuery("UPDATE test SET this = 'testVal' WHERE __key = ?", MetricNames.FAST_QUERIES_EXECUTED, 100);
    }

    @Test
    public void test_deleteRouting() {
        testQuery("DELETE FROM test WHERE __key = ?", MetricNames.FAST_QUERIES_EXECUTED, 100);
    }

    private void testQuery(String sql, String metricName, int keysNumber) {
        // wait for client to receive partition table
        assertTrueEventually(() -> {
            for (final Partition partition : client().getPartitionService().getPartitions()) {
                assertNotNull(partition.getOwner());
            }
        });

        // warm up cache
        client().getSql().execute(sql, 0);

        // collect pre-query-run metrics
        final List<Map<String, Object>> before = collectMetrics();
        final long[] expectedCounts = new long[instances().length];
        for (int i = 0; i < instances().length; i++) {
            expectedCounts[i] = (long) before.get(i).get(metricName);
        }

        // run queries
        for (long i = 1L; i < keysNumber; i++) {
            client().getSql().execute(sql, i);
            expectedCounts[getPartitionOwnerIndex(i)]++;
        }

        // assert
        final List<Map<String, Object>> after = collectMetrics();
        for (int i = 0; i < instances().length; i++) {
            assertEquals(expectedCounts[i], after.get(i).get(metricName));
        }
    }

    private int getPartitionOwnerIndex(Object key) {
        final Member owner = client().getPartitionService().getPartition(key).getOwner();
        for (int i = 0; i < instances().length; i++) {
            final HazelcastInstance instance = instances()[i];
            if (instance.getCluster().getLocalMember().getUuid().equals(owner.getUuid())) {
                return i;
            }
        }
        throw new HazelcastException("Partition Owner not found for key: " + key);
    }

    private List<Map<String, Object>> collectMetrics() {
        final List<Map<String, Object>> metricsList = new ArrayList<>();

        for (final HazelcastInstance instance : instances()) {
            final Map<String, Object> metrics = new HashMap<>();
            Accessors.getMetricsRegistry(instance).collect(new MetricsCollector() {
                @Override
                public void collectLong(final MetricDescriptor descriptor, final long value) {
                    if (MetricNames.MODULE_TAG.equals(descriptor.tagValue(MetricTags.MODULE))) {
                        metrics.put(descriptor.metric(), value);
                    }
                }

                @Override
                public void collectDouble(final MetricDescriptor descriptor, final double value) {
                    if (MetricNames.MODULE_TAG.equals(descriptor.tagValue(MetricTags.MODULE))) {
                        metrics.put(descriptor.metric(), value);
                    }
                }

                @Override
                public void collectException(final MetricDescriptor descriptor, final Exception e) {

                }

                @Override
                public void collectNoValue(final MetricDescriptor descriptor) {

                }
            });
            metricsList.add(metrics);
        }

        return metricsList;
    }
}
