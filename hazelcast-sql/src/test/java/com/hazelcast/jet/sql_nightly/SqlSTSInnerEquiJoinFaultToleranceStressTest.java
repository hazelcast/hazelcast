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

package com.hazelcast.jet.sql_nightly;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.JetService;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.impl.AbstractJobProxy;
import com.hazelcast.jet.impl.JetServiceBackend;
import com.hazelcast.jet.kafka.impl.KafkaTestSupport;
import com.hazelcast.jet.sql.SqlTestSupport;
import com.hazelcast.jet.sql.impl.connector.file.FileSqlConnector;
import com.hazelcast.jet.sql.impl.connector.kafka.KafkaSqlConnector;
import com.hazelcast.sql.SqlResult;
import com.hazelcast.sql.SqlRow;
import com.hazelcast.sql.SqlService;
import com.hazelcast.sql.SqlStatement;
import com.hazelcast.test.HazelcastParametrizedRunner;
import com.hazelcast.test.HazelcastSerialParametersRunnerFactory;
import com.hazelcast.test.annotation.NightlyTest;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static com.hazelcast.jet.core.JobStatus.FAILED;
import static com.hazelcast.jet.impl.util.ExceptionUtil.sneakyThrow;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.CSV_FORMAT;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_FORMAT;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_KEY_FORMAT;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_VALUE_FORMAT;
import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParametrizedRunner.class)
@UseParametersRunnerFactory(HazelcastSerialParametersRunnerFactory.class)
@Category(NightlyTest.class)
public class SqlSTSInnerEquiJoinFaultToleranceStressTest extends SqlTestSupport {
    private static final File RESOURCES_PATH = Paths.get("src/test/resources").toFile();

    private static final int INITIAL_PARTITION_COUNT = 1;
    private static final int EVENTS_PER_SINK = 25_000;
    private static final int SINK_ATTEMPTS = 10;
    protected static final int EVENTS_TO_PROCESS = EVENTS_PER_SINK * SINK_ATTEMPTS;
    protected static final int SNAPSHOT_TIMEOUT_SECONDS = 30;

    protected static final String JOB_NAME = "s2s_join";
    protected static final String EXACTLY_ONCE = "exactlyOnce";
    protected static final String AT_LEAST_ONCE = "atLeastOnce";
    private static KafkaTestSupport kafkaTestSupport;
    private Throwable ex;

    private SqlService sqlService;
    private Thread kafkaFeedThread;
    private String topicName;
    private File resultFile;
    protected String resultFileName;

    protected String fetchingQuery;

    protected HashSet<Object> expectedResultSet;
    protected int expectedEventsCount = EVENTS_TO_PROCESS;

    @Parameter(value = 0)
    public String processingGuarantee;

    @Parameter(value = 1)
    public boolean restartGraceful;

    @Parameterized.Parameters(name = "processingGuarantee:{0}, restartGraceful:{1}")
    public static Collection<Object[]> parameters() {
        return asList(new Object[][]{
                {AT_LEAST_ONCE, true},
                {AT_LEAST_ONCE, false},
                {EXACTLY_ONCE, true},
                {EXACTLY_ONCE, false}
        });
    }

    @BeforeClass
    public static void beforeClass() throws IOException {
        initialize(5, null);
        kafkaTestSupport = KafkaTestSupport.create();
        kafkaTestSupport.createKafkaCluster();
    }

    @AfterClass
    public static void afterClass() {
        kafkaTestSupport.shutdownKafkaCluster();
    }

    @Before
    public void setUp() throws Exception {
        sqlService = instance().getSql();

        // File sink definition
        resultFileName = randomName();
        sqlService.execute("CREATE MAPPING " + resultFileName + " ("
                + "id BIGINT EXTERNAL NAME __key"
                + ", string_id VARCHAR EXTERNAL NAME this"
                + ") TYPE " + FileSqlConnector.TYPE_NAME + ' '
                + "OPTIONS ("
                + '\'' + OPTION_FORMAT + "'='" + CSV_FORMAT + '\''
                + ", '" + FileSqlConnector.OPTION_PATH + "'='" + RESOURCES_PATH.getAbsolutePath() + '\''
                + ", '" + FileSqlConnector.OPTION_GLOB + "'='" + resultFileName + "." + CSV_FORMAT + '\''
                + ")"
        );

        // Kafka source definition
        topicName = createRandomTopic();
        sqlService.execute("CREATE MAPPING " + topicName + ' '
                + "TYPE " + KafkaSqlConnector.TYPE_NAME + ' '
                + "OPTIONS ( "
                + '\'' + OPTION_KEY_FORMAT + "'='int'"
                + ", '" + OPTION_VALUE_FORMAT + "'='varchar'"
                + ", 'bootstrap.servers'='" + kafkaTestSupport.getBrokerConnectionString() + '\''
                + ", 'auto.offset.reset'='earliest'"
                + ")");

        kafkaFeedThread = new Thread(() -> createTopicData(sqlService, topicName));
        kafkaFeedThread.start();

        // Expected result set fulfillment
        expectedResultSet = new HashSet<>();
        for (int i = 1; i <= expectedEventsCount; i++) {
            expectedResultSet.add(i);
        }

        // Left & right sides of the JOIN
        sqlService.execute("CREATE VIEW s1 AS " +
                "SELECT * FROM TABLE(IMPOSE_ORDER(TABLE " + topicName + " , DESCRIPTOR(__key), 10))");
        sqlService.execute("CREATE VIEW s2 AS " +
                "SELECT * FROM TABLE(IMPOSE_ORDER(TABLE " + topicName + " , DESCRIPTOR(__key), 5))");

        fetchingQuery = setupFetchingQuery();

        // File to sank preparation
        resultFile = File.createTempFile(resultFileName, "." + CSV_FORMAT, RESOURCES_PATH);
        assertTrue(resultFile.exists());
        assertTrue(resultFile.canWrite());

        Thread.sleep(30_000L); // time to feed Kafka
    }

    @Override
    @After
    public void tearDown() {
        assertTrue(resultFile.delete());
        super.tearDown();
    }

    @Test(timeout = 600_000L)
    public void stressTest() {
        JobRestarter jobRestarter = new JobRestarter(instance());
        jobRestarter.start();

        processAndCheckResults(jobRestarter);

        try {
            kafkaFeedThread.join();
            jobRestarter.join();
        } catch (AssertionError e) {
            Assert.fail(e.getMessage());
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    protected void processAndCheckResults(JobRestarter jobRestarter) {
        CompletableFuture<Integer> queryFinished = assertQueryCompleted(fetchingQuery, EVENTS_TO_PROCESS);
        assertFalse(queryFinished.isCompletedExceptionally());
        jobRestarter.finish();

        String mainCheckQuery = "SELECT id FROM " + resultFileName;
        try (SqlResult result = sqlService.execute(mainCheckQuery)) {
            Iterator<SqlRow> iterator = result.iterator();
            while (iterator.hasNext()) {
                Integer id = iterator.next().getObject(0);
                assertTrue(expectedResultSet.remove(id));
            }
        }

        assertTrue(expectedResultSet.size() + " elements left", expectedResultSet.isEmpty());

        if (processingGuarantee.equals(EXACTLY_ONCE)) {
            String exactlyOnceCheckQuery = "SELECT id FROM " + resultFileName + " GROUP BY id HAVING COUNT(id) > 1";
            SqlResult result = sqlService.execute(exactlyOnceCheckQuery);
            assertFalse(result.iterator().hasNext());
        }
    }

    private void createTopicData(SqlService sqlService, String topicName) {
        int itemsSank = 0;
        for (int sink = 1; sink <= SINK_ATTEMPTS; sink++) {
            StringBuilder queryBuilder = new StringBuilder("INSERT INTO " + topicName + " VALUES ");
            for (int i = 1; i <= EVENTS_PER_SINK; ++i) {
                ++itemsSank;
                queryBuilder.append("(").append(itemsSank).append(", 'value-").append(itemsSank).append("'), ");
            }
            queryBuilder.append("(").append(itemsSank).append(", 'value-").append(itemsSank).append("')");

            assertEquals(itemsSank, EVENTS_PER_SINK * sink);
            sqlService.execute(queryBuilder.toString());
            System.err.println("Items sank " + itemsSank);
        }
    }

    protected String setupFetchingQuery() {
        return "CREATE JOB " + JOB_NAME +
                " OPTIONS ('processingGuarantee'='" + processingGuarantee + "', 'snapshotIntervalMillis' = '1000')" +
                " AS SINK INTO " + resultFileName +
                " SELECT s1.__key, s2.this FROM s1 JOIN s2 ON s1.__key = s2.__key";
    }

    protected CompletableFuture<Integer> assertQueryCompleted(String sql, int elements) {
        SqlService sqlService = instance().getSql();
        CompletableFuture<Integer> future = new CompletableFuture<>();

        Thread thread = new Thread(() -> {
            SqlStatement statement = new SqlStatement(sql);

            try (SqlResult result = sqlService.execute(statement)) {
                int resultSetSize = 0;
                while ((resultSetSize = countResults()) < elements + 1) {
                    Thread.sleep(1000L);
                }

                future.complete(resultSetSize);
            } catch (Throwable e) {
                e.printStackTrace();
                future.completeExceptionally(e);
            }
        });

        thread.start();

        try {
            try {
                future.get(5, TimeUnit.MINUTES);
            } catch (TimeoutException e) {
                thread.interrupt();
                thread.join();
            }
        } catch (Exception e) {
            throw sneakyThrow(e);
        }

        int resultSetSize = 0;
        try {
            resultSetSize = countResults();
        } catch (IOException e) {
            throw sneakyThrow(e);
        }

        assertThat(resultSetSize).isGreaterThanOrEqualTo(elements);
        return future;
    }

    private int countResults() throws IOException {
        BufferedReader br = new BufferedReader(new FileReader(resultFileName));
        int cnt = 0;
        while (br.readLine() != null) {
            cnt++;
        }
        br.close();
        return cnt;
    }

    private static String createRandomTopic() {
        String topicName = randomName();
        kafkaTestSupport.createTopic(topicName, INITIAL_PARTITION_COUNT);
        return topicName;
    }

    class JobRestarter extends Thread {
        private final JetService jetService;
        private final JetServiceBackend jetBackend;
        private volatile boolean finish;

        JobRestarter(HazelcastInstance hazelcastInstance) {
            this.jetService = hazelcastInstance.getJet();
            this.jetBackend = getJetServiceBackend(hazelcastInstance);
        }

        @Override
        public void run() {
            while (jetService.getJob(JOB_NAME) == null) {
                try {
                    Thread.sleep(100L); // wait for query start
                } catch (InterruptedException e) {
                    ex = e;
                }
            }

            for (; ; ) {
                if (finish) {
                    return;
                }
                AbstractJobProxy job = (AbstractJobProxy) jetService.getJob(JOB_NAME);
                waitForNextSnapshot(jetBackend.getJobRepository(), job.getId(), SNAPSHOT_TIMEOUT_SECONDS, false);
                job.restart(restartGraceful);
            }
        }

        public void finish() {
            Job job = jetService.getJob(JOB_NAME);
            assertNotNull(job);
            job.cancel();
            assertJobStatusEventually(job, FAILED);
            this.finish = true;
        }
    }
}
