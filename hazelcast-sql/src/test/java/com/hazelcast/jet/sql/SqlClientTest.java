/*
 * Copyright 2021 Hazelcast Inc.
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
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.sql.impl.connector.test.TestBatchSqlConnector;
import com.hazelcast.jet.sql.impl.connector.test.TestFailingSqlConnector;
import com.hazelcast.sql.SqlResult;
import com.hazelcast.sql.SqlRow;
import com.hazelcast.sql.SqlService;
import com.hazelcast.sql.impl.client.SqlClientService;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.BitSet;
import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicReference;

import static com.hazelcast.jet.core.JobStatus.FAILED;
import static com.hazelcast.jet.core.JobStatus.RUNNING;
import static com.hazelcast.jet.core.TestProcessors.streamingDag;
import static java.util.stream.Collectors.toList;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class SqlClientTest extends SqlTestSupport {

    @BeforeClass
    public static void setUpClass() {
        initialize(2, null);
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

    @Test
    public void when_memberShuttingDown_then_retriesWithAnotherMember() throws Exception {
        // Use a standalone cluster in this test - we shut down one of th members
        HazelcastInstance inst1 = createHazelcastInstance();
        HazelcastInstance inst2 = createHazelcastInstance();
        HazelcastInstance client = createHazelcastClient();

        Job job = inst1.getJet().newLightJob(streamingDag(), new JobConfig().setPreventShutdown(true));
        assertTrueEventually(() -> assertJobExecuting(job, inst2));
        Future<?> shutdownFuture = spawn(inst2::shutdown);

        // inst2 is now shutting down, but will not shut down until job is cancelled.
        // Try a couple of times - the client picks a random member, we need to ensure that
        // at least once it pick the shutting-down one.
        try {
            for (int i = 0; i < 10; i++) {
                logger.info("Executing query " + i + "...");
                SqlResult result = client.getSql().execute("select * from table(generate_series(0, -1))");
                assertFalse(result.iterator().hasNext());
            }
        } finally {
            job.cancel();
            shutdownFuture.get();
        }

        assertEquals(0, ((SqlClientService) client.getSql()).numberOfShuttingDownMembers());
    }

    private static Job awaitSingleRunningJob(HazelcastInstance hz) {
        AtomicReference<Job> job = new AtomicReference<>();
        assertTrueEventually(() -> {
            List<Job> jobs = hz.getJet().getJobs().stream().filter(j -> j.getStatus() == RUNNING).collect(toList());
            assertEquals(1, jobs.size());
            job.set(jobs.get(0));
        });
        return job.get();
    }
}
