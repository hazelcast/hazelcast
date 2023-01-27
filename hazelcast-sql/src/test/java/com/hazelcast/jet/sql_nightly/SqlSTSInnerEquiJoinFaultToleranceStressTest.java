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
import com.hazelcast.jet.sql.impl.connector.kafka.KafkaSqlConnector;
import com.hazelcast.sql.SqlResult;
import com.hazelcast.sql.SqlRow;
import com.hazelcast.sql.SqlService;
import com.hazelcast.sql.SqlStatement;
import com.hazelcast.test.HazelcastParametrizedRunner;
import com.hazelcast.test.HazelcastSerialParametersRunnerFactory;
import com.hazelcast.test.annotation.NightlyTest;
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

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.hazelcast.jet.core.JobStatus.FAILED;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_KEY_FORMAT;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_VALUE_FORMAT;
import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

@RunWith(HazelcastParametrizedRunner.class)
@UseParametersRunnerFactory(HazelcastSerialParametersRunnerFactory.class)
@Category(NightlyTest.class)
public class SqlSTSInnerEquiJoinFaultToleranceStressTest extends SqlTestSupport {
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
    private String sourceTopicName;
    protected String sinkTopic;

    protected String fetchingQuery;

    protected Map<String, Integer> resultSet = new HashMap<>();
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

        // Kafka source definition
        sourceTopicName = createRandomTopic();
        sqlService.execute("CREATE MAPPING " + sourceTopicName + ' '
                + "TYPE " + KafkaSqlConnector.TYPE_NAME + ' '
                + "OPTIONS ( "
                + '\'' + OPTION_KEY_FORMAT + "'='int'"
                + ", '" + OPTION_VALUE_FORMAT + "'='varchar'"
                + ", 'bootstrap.servers'='" + kafkaTestSupport.getBrokerConnectionString() + '\''
                + ", 'auto.offset.reset'='earliest'"
                + ")");

        kafkaFeedThread = new Thread(() -> createTopicData(sqlService, sourceTopicName));
        kafkaFeedThread.start();

        // Kafka sink topic definition
        sinkTopic = randomName();
        sqlService.execute("CREATE MAPPING " + sinkTopic
                + " TYPE " + KafkaSqlConnector.TYPE_NAME + ' '
                + " OPTIONS ( "
                + '\'' + OPTION_KEY_FORMAT + "'='int'"
                + ", '" + OPTION_VALUE_FORMAT + "'='varchar'"
                + ", 'bootstrap.servers'='" + kafkaTestSupport.getBrokerConnectionString() + '\''
                + ", 'auto.offset.reset'='earliest'"
                + ")");

        // Left & right sides of the JOIN
        sqlService.execute("CREATE VIEW s1 AS " +
                "SELECT * FROM TABLE(IMPOSE_ORDER(TABLE " + sourceTopicName + " , DESCRIPTOR(__key), 10))");
        sqlService.execute("CREATE VIEW s2 AS " +
                "SELECT * FROM TABLE(IMPOSE_ORDER(TABLE " + sourceTopicName + " , DESCRIPTOR(__key), 5))");

        fetchingQuery = setupFetchingQuery();

        Thread.sleep(15_000L); // time to feed Kafka
    }

    @Test
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
        Thread thread = new Thread(() -> {
            SqlStatement statement = new SqlStatement(fetchingQuery);

            try (SqlResult result = sqlService.execute(statement)) {
            } catch (Throwable e) {
                e.printStackTrace();
            }
        });
        thread.start();

        String mainCheckQuery = "SELECT * FROM " + sinkTopic;
        try (SqlResult result = sqlService.execute(mainCheckQuery)) {
            for (SqlRow sqlRow : result) {
                String s = sqlRow.getObject(1);
                resultSet.compute(s, (str, i) -> i == null ? 0 : i + 1);
                if (resultSet.size() >= EVENTS_TO_PROCESS) {
                    break;
                }
            }
        }

        try {
            thread.join();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        assertThat(resultSet.size()).isGreaterThanOrEqualTo(EVENTS_TO_PROCESS);
        jobRestarter.finish();

        if (processingGuarantee.equals(EXACTLY_ONCE)) {
            List<Integer> dups = resultSet.values().stream().filter(cnt -> cnt > 1).collect(Collectors.toList());
            for (Integer i : dups) {
                System.err.println(i);
            }
            assertThat(dups.size()).isZero();
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
                " AS SINK INTO " + sinkTopic +
                " SELECT s1.__key, s2.this FROM s1 JOIN s2 ON s1.__key = s2.__key";
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
