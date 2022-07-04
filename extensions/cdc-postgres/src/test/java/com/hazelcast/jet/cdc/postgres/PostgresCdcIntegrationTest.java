/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.cdc.postgres;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.accumulator.LongAccumulator;
import com.hazelcast.jet.cdc.CdcSinks;
import com.hazelcast.jet.cdc.ChangeRecord;
import com.hazelcast.jet.cdc.Operation;
import com.hazelcast.jet.cdc.ParsingException;
import com.hazelcast.jet.cdc.RecordPart;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.config.ProcessingGuarantee;
import com.hazelcast.jet.core.JetTestSupport;
import com.hazelcast.jet.core.JobStatus;
import com.hazelcast.jet.impl.JobProxy;
import com.hazelcast.jet.impl.JobRepository;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.StreamSource;
import com.hazelcast.map.IMap;
import com.hazelcast.test.annotation.NightlyTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import javax.annotation.Nonnull;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;

import static com.hazelcast.jet.Util.entry;
import static com.hazelcast.jet.core.JobStatus.RUNNING;
import static com.hazelcast.jet.impl.util.Util.uncheckRun;
import static java.lang.String.format;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.fail;

public class PostgresCdcIntegrationTest extends AbstractPostgresCdcIntegrationTest {

    @Test
    @Category(QuickTest.class)
    public void customers() throws Exception {
        // given
        List<String> expectedRecords = Arrays.asList(
                "1001/00000:SYNC:" + new Customer(1001, "Sally", "Thomas", "sally.thomas@acme.com"),
                "1002/00000:SYNC:" + new Customer(1002, "George", "Bailey", "gbailey@foobar.com"),
                "1003/00000:SYNC:" + new Customer(1003, "Edward", "Walker", "ed@walker.com"),
                "1004/00000:SYNC:" + new Customer(1004, "Anne", "Kretchmar", "annek@noanswer.org"),
                "1004/00001:UPDATE:" + new Customer(1004, "Anne Marie", "Kretchmar", "annek@noanswer.org"),
                "1005/00000:INSERT:" + new Customer(1005, "Jason", "Bourne", "jason@bourne.org"),
                "1005/00001:DELETE:" + new Customer(1005, "Jason", "Bourne", "jason@bourne.org")
        );

        Pipeline pipeline = customersPipeline(null);

        // when
        HazelcastInstance hz = createHazelcastInstances(2)[0];
        Job job = hz.getJet().newJob(pipeline);

        //then
        assertEqualsEventually(() -> hz.getMap("results").size(), 4);

        //when
        executeBatch(
                "UPDATE customers SET first_name='Anne Marie' WHERE id=1004",
                "INSERT INTO customers VALUES (1005, 'Jason', 'Bourne', 'jason@bourne.org')",
                "DELETE FROM customers WHERE id=1005"
        );

        //then
        try {
            assertEqualsEventually(() -> mapResultsToSortedList(hz.getMap("results")), expectedRecords);
        } finally {
            job.cancel();
            assertJobStatusEventually(job, JobStatus.FAILED);
        }
    }

    @Test
    @Category(NightlyTest.class)
    public void orders() {
        // given
        List<String> expectedRecords = Arrays.asList(
                "10001/0:SYNC:" + new Order(10001, new Date(1452902400000L), 1001, 1, 102),
                "10002/0:SYNC:" + new Order(10002, new Date(1452988800000L), 1002, 2, 105),
                "10003/0:SYNC:" + new Order(10003, new Date(1455840000000L), 1002, 2, 106),
                "10004/0:SYNC:" + new Order(10004, new Date(1456012800000L), 1003, 1, 107)
        );

        Pipeline pipeline = ordersPipeline();

        // when
        HazelcastInstance hz = createHazelcastInstances(2)[0];
        Job job = hz.getJet().newJob(pipeline);

        //then
        try {
            assertEqualsEventually(() -> mapResultsToSortedList(hz.getMap("results")), expectedRecords);
        } finally {
            job.cancel();
            assertJobStatusEventually(job, JobStatus.FAILED);
        }
    }

    @Nonnull
    private Pipeline ordersPipeline() {
        Pipeline pipeline = Pipeline.create();
        pipeline.readFrom(source("orders"))
                .withoutTimestamps()
                .groupingKey(PostgresCdcIntegrationTest::getOrderNumber)
                .mapStateful(
                        LongAccumulator::new,
                        (accumulator, orderId, record) -> {
                            long count = accumulator.get();
                            accumulator.add(1);
                            Operation operation = record.operation();
                            RecordPart value = record.value();
                            Order order = value.toObject(Order.class);
                            return entry(orderId + "/" + count, operation + ":" + order);
                        })
                .setLocalParallelism(1)
                .writeTo(Sinks.map("results"));
        return pipeline;
    }

    @Test
    @Category(NightlyTest.class)
    public void stressTest_exactlyOnce_forcefulRestart() throws Exception {
        stressTest_exactlyOnce(false);
    }

    @Test
    @Category(NightlyTest.class)
    public void stressTest_exactlyOnce_gracefulRestart() throws Exception {
        stressTest_exactlyOnce(true);
    }

    public void stressTest_exactlyOnce(boolean graceful) throws Exception {
        int updates = 1000;
        int restarts = 10;
        int snapshotIntervalMs = 100;

        Pipeline pipeline = customersPipeline(Long.MAX_VALUE);

        JobConfig config = new JobConfig()
                .setProcessingGuarantee(ProcessingGuarantee.EXACTLY_ONCE)
                .setSnapshotIntervalMillis(snapshotIntervalMs);

        HazelcastInstance hz = createHazelcastInstances(2)[0];
        Job job = hz.getJet().newJob(pipeline, config);
        JetTestSupport.assertJobStatusEventually(job, JobStatus.RUNNING);
        assertEqualsEventually(() -> hz.getMap("results").size(), 4);

        //make sure the job stores a Postgres WAL offset so that it won't trigger database snapshots after any restart
        //multiple snapshots are a problem for this test, because it is updating the same row, so subsequent snapshots
        //will return different images
        JobRepository jr = new JobRepository(hz);
        waitForNextSnapshot(jr, job.getId(), 20, false);

        String lsnFlushedBeforeRestart = getConfirmedFlushLsn();

        Future<?> dbChangesFuture = spawn(() -> uncheckRun(() -> {
            for (int i = 1; i <= updates; i++) {
                executeBatch("UPDATE customers SET first_name='Anne" + i + "' WHERE id=1004");
            }
        }));

        for (int i = 0; i < restarts; i++) {
            ((JobProxy) job).restart(graceful);
            assertJobStatusEventually(job, RUNNING);

            Thread.sleep(ThreadLocalRandom.current().nextInt(snapshotIntervalMs * 2));
        }

        JetTestSupport.assertJobStatusEventually(job, JobStatus.RUNNING);

        try {
            List<String> expectedPatterns = new ArrayList<>(Arrays.asList(
                    "1001/00000:SYNC:Customer \\{id=1001, firstName=Sally, lastName=Thomas, "
                            + "email=sally.thomas@acme.com\\}",
                    "1002/00000:SYNC:Customer \\{id=1002, firstName=George, lastName=Bailey, "
                            + "email=gbailey@foobar.com\\}",
                    "1003/00000:SYNC:Customer \\{id=1003, firstName=Edward, lastName=Walker, "
                            + "email=ed@walker.com\\}",
                    "1004/00000:SYNC:Customer \\{id=1004, firstName=Anne, lastName=Kretchmar, "
                            + "email=annek@noanswer.org\\}"
            ));
            for (int i = 1; i <= updates; i++) {
                expectedPatterns.add("1004/" + format("%05d", i) + ":UPDATE:Customer \\{id=1004, firstName=Anne" + i +
                        ", lastName=Kretchmar, email=annek@noanswer.org\\}");
            }
            assertTrueEventually(() -> assertMatch(expectedPatterns, mapResultsToSortedList(hz.getMap("results"))));
            assertTrueEventually(() -> assertNotEquals(lsnFlushedBeforeRestart, getConfirmedFlushLsn()));
        } finally {
            job.cancel();
            assertJobStatusEventually(job, JobStatus.FAILED);
            dbChangesFuture.get();
        }
    }

    @Test
    @Category(QuickTest.class)
    public void restart_noProcessingGuarantee() throws Exception {
        Pipeline pipeline = customersPipeline(250L);

        // when
        HazelcastInstance hz = createHazelcastInstances(2)[0];
        Job job = hz.getJet().newJob(pipeline, new JobConfig());
        JetTestSupport.assertJobStatusEventually(job, JobStatus.RUNNING);
        assertEqualsEventually(() -> hz.getMap("results").size(), 4);

        //then update a record
        executeBatch("INSERT INTO customers VALUES (1005, 'Jason', 'Bourne', 'jason@bourne.org')");

        //when
        assertEqualsEventually(() -> hz.getMap("results").size(), 5);

        //then
        hz.getMap("results").destroy();

        //when
        assertEqualsEventually(() -> hz.getMap("results").size(), 0);

        String lsnFlushedBeforeRestart = getConfirmedFlushLsn();

        //then
        job.restart();

        //when
        JetTestSupport.assertJobStatusEventually(job, JobStatus.RUNNING);
        assertEqualsEventually(() -> hz.getMap("results").size(), 5);

        //then update a record
        executeBatch(
                "UPDATE customers SET first_name='Anne Marie' WHERE id=1004",
                "DELETE FROM customers WHERE id=1005"
        );

        //then
        try {
            assertEqualsEventually(() -> mapResultsToSortedList(hz.getMap("results")),
                    Arrays.asList(
                            "1001/00000:SYNC:" + new Customer(1001, "Sally", "Thomas", "sally.thomas@acme.com"),
                            "1002/00000:SYNC:" + new Customer(1002, "George", "Bailey", "gbailey@foobar.com"),
                            "1003/00000:SYNC:" + new Customer(1003, "Edward", "Walker", "ed@walker.com"),
                            "1004/00000:SYNC:" + new Customer(1004, "Anne", "Kretchmar", "annek@noanswer.org"),
                            "1004/00001:UPDATE:" + new Customer(1004, "Anne Marie", "Kretchmar", "annek@noanswer.org"),
                            "1005/00000:SYNC:" + new Customer(1005, "Jason", "Bourne", "jason@bourne.org"),
                            "1005/00001:DELETE:" + new Customer(1005, "Jason", "Bourne", "jason@bourne.org")
                    ));
            assertTrueEventually(() -> assertNotEquals(lsnFlushedBeforeRestart, getConfirmedFlushLsn()));
        } finally {
            job.cancel();
            assertJobStatusEventually(job, JobStatus.FAILED);
        }
    }

    @Test
    @Category(NightlyTest.class)
    public void dataLoss() throws Exception {
        int offset = 1005;
        int length = 9995;

        // given
        List<String> expectedRecords = new ArrayList<>(Arrays.asList(
                "1001/00000:(SYNC|INSERT):Customer \\{id=1001, firstName=Sally, lastName=Thomas, "
                        + "email=sally.thomas@acme.com\\}",
                "1002/00000:(SYNC|INSERT):Customer \\{id=1002, firstName=George, lastName=Bailey, "
                        + "email=gbailey@foobar.com\\}",
                "1003/00000:(SYNC|INSERT):Customer \\{id=1003, firstName=Edward, lastName=Walker, "
                        + "email=ed@walker.com\\}",
                "1004/00000:(SYNC|INSERT):Customer \\{id=1004, firstName=Anne, lastName=Kretchmar, "
                        + "email=annek@noanswer.org\\}"
        ));
        for (int i = offset; i < offset + length; i++) {
            expectedRecords.add(i + "/00000:(SYNC|INSERT):Customer \\{id=" + i + ", firstName=first" + i +
                    ", lastName=last" + i + ", email=" + i + "@google.com\\}");
        }
        expectedRecords.sort(String::compareTo);

        Pipeline pipeline = customersPipeline(null);

        // when
        HazelcastInstance hz = createHazelcastInstances(1)[0];
        Job job = hz.getJet().newJob(pipeline);

        //then
        assertJobStatusEventually(job, JobStatus.RUNNING);

        //when
        String[] batch = new String[length];
        for (int i = offset; i < offset + length; i++) {
            batch[i - offset] = "INSERT INTO customers VALUES (" + i + ", 'first" + i + "', 'last" + i + "', '"
                    + i + "@google.com')";
        }
        executeBatch(batch);

        //then
        try {
            assertTrueEventually(() -> {
                IMap<Object, Object> map = hz.getMap("results");
                int size = map.size();
                System.out.println("No. of records: " + size);
                assertEquals(expectedRecords.size(), size);
                assertMatch(expectedRecords, mapResultsToSortedList(map));
            });
        } finally {
            job.cancel();
            assertJobStatusEventually(job, JobStatus.FAILED);
        }
    }

    @Nonnull
    private Pipeline customersPipeline(Long commitPeriod) {
        Pipeline pipeline = Pipeline.create();
        pipeline.readFrom(source("customers", commitPeriod))
                .withNativeTimestamps(0)
                .<ChangeRecord>customTransform("filter_timestamps", filterTimestampsProcessorSupplier())
                .groupingKey(record -> (Integer) record.key().toMap().get("id"))
                .mapStateful(
                        LongAccumulator::new,
                        (accumulator, customerId, record) -> {
                            String count = format("%05d", accumulator.get());
                            accumulator.add(1);
                            Operation operation = record.operation();
                            RecordPart value = record.value();
                            Customer customer = value.toObject(Customer.class);
                            return entry(customerId + "/" + count, operation + ":" + customer);
                        })
                .setLocalParallelism(1)
                .writeTo(Sinks.map("results"));
        return pipeline;
    }

    @Test
    @Category(NightlyTest.class)
    public void cdcMapSink() throws Exception {
        // given
        Pipeline pipeline = Pipeline.create();
        pipeline.readFrom(source("customers"))
                .withNativeTimestamps(0)
                .writeTo(CdcSinks.map("cache",
                        r -> r.key().toMap().get("id"),
                        r -> r.value().toObject(Customer.class).toString()));


        // when
        HazelcastInstance hz = createHazelcastInstances(2)[0];
        JobConfig jobConfig = new JobConfig().setProcessingGuarantee(ProcessingGuarantee.AT_LEAST_ONCE);
        Job job = hz.getJet().newJob(pipeline, jobConfig);
        JetTestSupport.assertJobStatusEventually(job, JobStatus.RUNNING);
        //then
        assertEqualsEventually(() -> mapResultsToSortedList(hz.getMap("cache")),
                Arrays.asList(
                        "1001:Customer {id=1001, firstName=Sally, lastName=Thomas, email=sally.thomas@acme.com}",
                        "1002:Customer {id=1002, firstName=George, lastName=Bailey, email=gbailey@foobar.com}",
                        "1003:Customer {id=1003, firstName=Edward, lastName=Walker, email=ed@walker.com}",
                        "1004:Customer {id=1004, firstName=Anne, lastName=Kretchmar, email=annek@noanswer.org}"
                )
        );

        //when
        job.restart();
        JetTestSupport.assertJobStatusEventually(job, JobStatus.RUNNING);
        executeBatch(
                "UPDATE customers SET first_name='Anne Marie' WHERE id=1004",
                "INSERT INTO customers VALUES (1005, 'Jason', 'Bourne', 'jason@bourne.org')"
        );
        //then
        assertEqualsEventually(() -> mapResultsToSortedList(hz.getMap("cache")),
                Arrays.asList(
                        "1001:Customer {id=1001, firstName=Sally, lastName=Thomas, email=sally.thomas@acme.com}",
                        "1002:Customer {id=1002, firstName=George, lastName=Bailey, email=gbailey@foobar.com}",
                        "1003:Customer {id=1003, firstName=Edward, lastName=Walker, email=ed@walker.com}",
                        "1004:Customer {id=1004, firstName=Anne Marie, lastName=Kretchmar, email=annek@noanswer.org}",
                        "1005:Customer {id=1005, firstName=Jason, lastName=Bourne, email=jason@bourne.org}"
                )
        );

        //when
        executeBatch("DELETE FROM customers WHERE id=1005");
        //then
        assertEqualsEventually(() -> mapResultsToSortedList(hz.getMap("cache")),
                Arrays.asList(
                        "1001:Customer {id=1001, firstName=Sally, lastName=Thomas, email=sally.thomas@acme.com}",
                        "1002:Customer {id=1002, firstName=George, lastName=Bailey, email=gbailey@foobar.com}",
                        "1003:Customer {id=1003, firstName=Edward, lastName=Walker, email=ed@walker.com}",
                        "1004:Customer {id=1004, firstName=Anne Marie, lastName=Kretchmar, email=annek@noanswer.org}"
                )
        );
    }

    private StreamSource<ChangeRecord> source(String tableName) {
        return source(tableName, null);
    }

    private StreamSource<ChangeRecord> source(String tableName, Long commitPeriod) {
        PostgresCdcSources.Builder builder = sourceBuilder(tableName);
        if (commitPeriod != null) {
            builder.setCommitPeriod(commitPeriod);
        }
        return builder
                .setReplicationSlotName(REPLICATION_SLOT_NAME)
                .setTableWhitelist("inventory." + tableName)
                .build();
    }

    private String getConfirmedFlushLsn() throws SQLException {
        try (Connection connection = getConnection(postgres)) {
            PreparedStatement preparedStatement = connection.prepareStatement(
                    "select * from pg_replication_slots where slot_name = ? and database = ?");
            preparedStatement.setString(1, REPLICATION_SLOT_NAME);
            preparedStatement.setString(2, DATABASE_NAME);
            ResultSet rs = preparedStatement.executeQuery();
            if (rs.next()) {
                return rs.getString("confirmed_flush_lsn");
            } else {
                fail("No replication slot info available");
                return null;
            }
        }
    }

    private static int getOrderNumber(ChangeRecord record) throws ParsingException {
        //pick random method for extracting ID in order to test all code paths
        boolean primitive = ThreadLocalRandom.current().nextBoolean();
        if (primitive) {
            return (Integer) record.key().toMap().get("id");
        } else {
            return record.key().toObject(OrderPrimaryKey.class).id;
        }
    }

    private static class OrderPrimaryKey {

        @JsonProperty("id")
        public int id;

        @Override
        public int hashCode() {
            return id;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }
            OrderPrimaryKey other = (OrderPrimaryKey) obj;
            return id == other.id;
        }

        @Override
        public String toString() {
            return "OrderPrimaryKey {id=" + id + '}';
        }
    }
}
