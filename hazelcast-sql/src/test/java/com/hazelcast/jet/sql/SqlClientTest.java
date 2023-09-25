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

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.impl.JetServiceBackend;
import com.hazelcast.jet.impl.execution.ExecutionContext;
import com.hazelcast.jet.sql.impl.connector.map.model.Person;
import com.hazelcast.jet.sql.impl.connector.test.TestBatchSqlConnector;
import com.hazelcast.jet.sql.impl.connector.test.TestFailingSqlConnector;
import com.hazelcast.map.IMap;
import com.hazelcast.sql.SqlResult;
import com.hazelcast.sql.SqlRow;
import com.hazelcast.sql.SqlService;
import com.hazelcast.sql.impl.client.SqlClientService;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import java.util.BitSet;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

import static com.hazelcast.jet.Util.idToString;
import static com.hazelcast.jet.core.JobStatus.FAILED;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;

public class SqlClientTest extends SqlTestSupport {

    @BeforeClass
    public static void setUpClass() {
        initializeWithClient(2, null, null);
    }

    @Test
    public void test_partitionBasedRouting_insert() {
        createMapping("test_simpleKey", Integer.class, String.class);
        createMapping("test_complexKey", Person.class, String.class);

        checkPartitionArgumentIndex("INSERT INTO test_simpleKey (__key, this) VALUES (?, ?)", 0, 1, "value");
        checkPartitionArgumentIndex("INSERT INTO test_simpleKey (this, __key) VALUES (?, ?)", 1, "value", 2);
        // no dynamic argument
        checkPartitionArgumentIndex("INSERT INTO test_simpleKey (this, __key) VALUES ('value', 3)", null);
        checkPartitionArgumentIndex("INSERT INTO test_simpleKey (this, __key) VALUES ('value', 4), ('value', 5)", null);
        // has dynamic argument, but multiple rows
        checkPartitionArgumentIndex("INSERT INTO test_simpleKey (this, __key) VALUES (?, ?), (?, ?)", null,
                "value", 6, "value", 7);

        // partition argument index not supported if `__key` isn't directly assigned to
        checkPartitionArgumentIndex("INSERT INTO test_complexKey (this, id, name) VALUES (?, ?, ?)", null,
                "value-1", 1, "name-1");
        // this test case is here just for completeness to show that we cannot support complex keys and partition argument
        assertThatThrownBy(() -> checkPartitionArgumentIndex("INSERT INTO test_complexKey (this, __key) VALUES (?, ?)", null,
                "value-1", new Person(2, "name-2")))
                .hasMessageEndingWith("Writing to top-level fields of type OBJECT not supported");
    }

    @Test
    public void test_partitionBasedRouting() {
        createMapping("test", Integer.class, String.class);
        createMapping("test2", Integer.class, String.class);

        checkPartitionArgumentIndex("SELECT * FROM test WHERE __key = ?", 0, 1);
        checkPartitionArgumentIndex("UPDATE test SET this = ? WHERE __key = ?", 1, "testVal", 1);
        checkPartitionArgumentIndex("DELETE FROM test WHERE __key = ?", 0, 1);
        checkPartitionArgumentIndex("SELECT JSON_OBJECT(this : __key) FROM test WHERE __key = ?", 0, 1);
        checkPartitionArgumentIndex("SELECT JSON_ARRAY(__key, this) FROM test WHERE __key = ?", 0, 1);

        // aggregation
        checkPartitionArgumentIndex("SELECT JSON_OBJECTAGG(this : __key) FROM test WHERE __key = ?", null, 1);
        checkPartitionArgumentIndex("SELECT SUM(__key) FROM test WHERE __key = ?", null, 1);
        checkPartitionArgumentIndex("SELECT COUNT(*) FROM test WHERE __key = ?", null, 1);
        // join
        checkPartitionArgumentIndex("SELECT * FROM test t1 JOIN test2 t2 ON t1.__key = t2.__key WHERE t1.__key = ?",
                null, 1);
        checkPartitionArgumentIndex("SELECT t1.*, t2.* FROM test t1 JOIN test2 t2 USING(__key) WHERE t1.__key = ?",
                null, 1);
    }

    @Test
    public void test_partitionBasedRoutingComplexKey() {
        createMapping("test_complex", Person.class, String.class);

        checkPartitionArgumentIndex("SELECT * FROM test_complex WHERE __key = ?",
                0, new Person(1, "name-1"));
        checkPartitionArgumentIndex("UPDATE test_complex SET this = ? WHERE __key = ?",
                1, "testVal", new Person(1, "name-1"));
        checkPartitionArgumentIndex("DELETE FROM test_complex WHERE __key = ?",
                0, new Person(1, "name-1"));
    }

    @Test
    @Ignore("Currently unsupported: https://github.com/hazelcast/hazelcast/pull/22659#issuecomment-1382086013")
    public void test_partitionBasedRouting_multipleConditions() {
        createMapping("test", Integer.class, String.class);
        createMapping("complex_value", Integer.class, Person.class);

        checkPartitionArgumentIndex("SELECT * FROM test WHERE __key = ? AND this = ?", 0, 1, "aaa");
        checkPartitionArgumentIndex("SELECT * FROM test WHERE this = ? AND __key = ?", 1, "aaa", 1);
        checkPartitionArgumentIndex("SELECT * FROM complex_value WHERE __key = ? AND id = ?",
                0, 1, 1);
        checkPartitionArgumentIndex("SELECT * FROM complex_value WHERE __key = ? AND id = ? AND name = ?",
                0, 1, 1, "name-1");
    }

    private void checkPartitionArgumentIndex(String sql, Integer expectedIndex, Object... arguments) {
        final SqlClientService sqlService = (SqlClientService) client().getSql();
        assertNull(sqlService.partitionArgumentIndexCache.get(sql));
        sqlService.execute(sql, arguments);
        assertEquals(expectedIndex, sqlService.partitionArgumentIndexCache.get(sql));
    }

    @Test
    public void test_jetJobReturnRowsToClientFrom() {
        HazelcastInstance client = factory().newHazelcastClient();
        SqlService sqlService = client.getSql();

        int itemCount = 10_000;

        TestBatchSqlConnector.create(sqlService, "t", itemCount);

        SqlResult result = sqlService.execute("SELECT v FROM t");
        BitSet seenValues = new BitSet(itemCount);
        for (SqlRow r : result) {
            Integer v = r.getObject(0);
            assertFalse("value already seen: " + v, seenValues.get(v));
            seenValues.set(v);
        }
        assertEquals(itemCount, seenValues.cardinality());
    }

    @Test
    public void when_clientDisconnects_then_jobCancelled() {
        HazelcastInstance client = factory().newHazelcastClient();
        SqlService sqlService = client.getSql();

        sqlService.execute("SELECT * FROM TABLE(GENERATE_STREAM(100))");
        Job job = awaitSingleRunningJob(instance());

        client.shutdown();
        assertJobStatusEventually(job, FAILED);
        assertThatThrownBy(job::join)
                .hasMessageContaining("QueryException: Client cannot be reached");
    }

    @Test
    public void when_jobFails_then_clientFindsOut() {
        HazelcastInstance client = factory().newHazelcastClient();
        SqlService sqlService = client.getSql();

        sqlService.execute("CREATE MAPPING t TYPE " + TestFailingSqlConnector.TYPE_NAME);
        assertThatThrownBy(
                () -> {
                    SqlResult result = sqlService.execute("SELECT * FROM t");
                    for (SqlRow r : result) {
                        System.out.println(r);
                    }
                })
                .hasMessageContaining("mock failure");
    }

    @Test
    public void when_resultClosed_then_jobCancelled_withNoResults() {
        /*
        There was an issue that RootResultConsumerSink didn't check for failures, unless
        it had some items in the inbox.
         */
        HazelcastInstance client = factory().newHazelcastClient();
        SqlService sqlService = client.getSql();

        logger.info("before select");
        SqlResult result = sqlService.execute("SELECT * FROM TABLE(GENERATE_STREAM(0))");
        logger.info("after execute returned");
        Job job = awaitSingleRunningJob(client);
        logger.info("Job is running.");

        result.close();
        logger.info("after res.close() returned");
        assertJobStatusEventually(job, FAILED);
    }

    // test for https://github.com/hazelcast/hazelcast/issues/19897
    @Test
    public void when_resultClosed_then_executionContextCleanedUp() {
        HazelcastInstance client = factory().newHazelcastClient();
        SqlService sql = client.getSql();

        IMap<Integer, Integer> map = instance().getMap("map");
        Map<Integer, Integer> tmpMap = new HashMap<>();
        for (int i = 0; i < 100_000; i++) {
            tmpMap.put(i, i);
            if (i % 10_000 == 0) {
                map.putAll(tmpMap);
                tmpMap.clear();
            }
        }

        createMapping("map", Integer.class, Integer.class);

        for (int i = 0; i < 100; i++) {
            SqlResult result = sql.execute("SELECT * FROM map");
            result.close();
        }

        JetServiceBackend jetService = getJetServiceBackend(instance());
        Collection<ExecutionContext> contexts = jetService.getJobExecutionService().getExecutionContexts();
        // Assert that all ExecutionContexts are eventually cleaned up
        // This assert will fail if a network packet arrives after the JobExecutionService#FAILED_EXECUTION_EXPIRY_NS
        // time. Hopefully Jenkins isn't that slow.
        assertTrueEventually(() -> {
            String remainingContexts = contexts.stream()
                    .map(c -> idToString(c.executionId()))
                    .collect(Collectors.joining(", "));
            assertEquals("remaining execIds: " + remainingContexts, 0, contexts.size());
        }, 5);

        // assert that failedJobs is also cleaned up
        ConcurrentMap<Long, Long> failedJobs = jetService.getJobExecutionService().getFailedJobs();
        assertTrueEventually(() -> assertEquals(0, failedJobs.size()));
    }
}
