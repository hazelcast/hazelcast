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
import com.hazelcast.jet.sql.impl.PlanExecutor;
import com.hazelcast.partition.Partition;
import com.hazelcast.test.HazelcastSerialClassRunner;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.Arrays;

import static org.junit.Assert.assertArrayEquals;
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
        testQuery("SELECT * FROM test WHERE __key = ?", 100);
    }

    @Test
    public void test_insertRouting() {
        testQuery("INSERT INTO test (this, __key) VALUES ('testVal', ?)", 100);
    }

    @Test
    public void test_updateRouting() {
        testQuery("UPDATE test SET this = 'testVal' WHERE __key = ?", 100);
    }

    @Test
    public void test_deleteRouting() {
        testQuery("DELETE FROM test WHERE __key = ?", 100);
    }

    private void testQuery(String sql, int keyCount) {
        // wait for client to receive partition table
        assertTrueEventually(() -> {
            for (final Partition partition : client().getPartitionService().getPartitions()) {
                assertNotNull(partition.getOwner());
            }
        });

        // warm up cache
        client().getSql().execute(sql, 0);

        // collect pre-execution metrics
        final long[] expectedCounts = Arrays.stream(instances())
                .mapToLong(inst -> getPlanExecutor(inst).getDirectIMapQueriesExecuted())
                .toArray();

        // run queries
        for (long i = 1L; i < keyCount; i++) {
            client().getSql().execute(sql, i);
            expectedCounts[getPartitionOwnerIndex(i)]++;
        }

        // assert
        final long[] actualCounts = Arrays.stream(instances())
                .mapToLong(inst -> getPlanExecutor(inst).getDirectIMapQueriesExecuted())
                .toArray();
        assertArrayEquals(expectedCounts, actualCounts);
    }

    private static PlanExecutor getPlanExecutor(HazelcastInstance instance) {
        return sqlServiceImpl(instance).getOptimizer().getPlanExecutor();
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
}
