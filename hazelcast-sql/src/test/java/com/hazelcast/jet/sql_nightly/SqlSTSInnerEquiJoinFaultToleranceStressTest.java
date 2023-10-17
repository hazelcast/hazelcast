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
import com.hazelcast.test.HazelcastParametrizedRunner;
import com.hazelcast.test.HazelcastSerialParametersRunnerFactory;
import com.hazelcast.test.annotation.NightlyTest;
import com.hazelcast.test.annotation.ParallelJVMTest;
import org.junit.After;
import org.junit.AfterClass;
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
import java.util.Map.Entry;
import java.util.stream.Collectors;

import static com.hazelcast.jet.core.JobStatus.FAILED;
import static com.hazelcast.jet.core.JobStatus.RUNNING;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_KEY_FORMAT;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_VALUE_FORMAT;
import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

@RunWith(HazelcastParametrizedRunner.class)
@UseParametersRunnerFactory(HazelcastSerialParametersRunnerFactory.class)
@Category({NightlyTest.class, ParallelJVMTest.class})
public class SqlSTSInnerEquiJoinFaultToleranceStressTest extends SqlTestSupport {
    protected static final int SNAPSHOT_TIMEOUT_SECONDS = 30;
    protected static final String JOB_NAME = "s2s_join";
    protected static final String EXACTLY_ONCE = "exactlyOnce";
    protected static final String AT_LEAST_ONCE = "atLeastOnce";

    protected final int eventsPerSink = 500;
    protected int sinkCount = 400;
    protected int eventsToProcess = eventsPerSink * sinkCount;

    private static KafkaTestSupport kafkaTestSupport;
    private volatile Throwable ex;

    private final Map<String, Integer> resultSet = new HashMap<>();
    private SqlService sqlService;
    private Thread kafkaFeedThread;
    private String sourceTopic;
    protected String sinkTopic;
    private JobRestarter jobRestarter;

    protected int expectedEventsCount = eventsToProcess;
    protected int firstItemId = 1;
    protected int lastItemId = eventsToProcess;

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
        sourceTopic = "source_topic_" + randomName();
        kafkaTestSupport.createTopic(sourceTopic, 5);
        sqlService.execute("CREATE MAPPING " + sourceTopic + ' '
                + "TYPE " + KafkaSqlConnector.TYPE_NAME + ' '
                + "OPTIONS ( "
                + '\'' + OPTION_KEY_FORMAT + "'='int'"
                + ", '" + OPTION_VALUE_FORMAT + "'='varchar'"
                + ", 'bootstrap.servers'='" + kafkaTestSupport.getBrokerConnectionString() + '\''
                + ", 'auto.offset.reset'='earliest'"
                + ")");

        kafkaFeedThread = new Thread(() -> createTopicData(sqlService, sourceTopic));
        kafkaFeedThread.start();

        // Kafka sink topic definition
        sinkTopic = "sink_topic_" + randomName();
        kafkaTestSupport.createTopic(sinkTopic, 5);
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
                "SELECT * FROM TABLE(IMPOSE_ORDER(TABLE " + sourceTopic + " , DESCRIPTOR(__key), 3))");
        sqlService.execute("CREATE VIEW s2 AS " +
                "SELECT * FROM TABLE(IMPOSE_ORDER(TABLE " + sourceTopic + " , DESCRIPTOR(__key), 4))");

        jobRestarter = new JobRestarter(instance());
        jobRestarter.start();
    }

    @After
    public void after() throws InterruptedException {
        kafkaFeedThread.join();
        kafkaFeedThread = null;

        // finish again to make sure the thread terminates in every case
        jobRestarter.finish();
        jobRestarter.join();
        jobRestarter = null;

        if (ex != null) {
            throw new RuntimeException(ex);
        }
    }

    @Test(timeout = 1_200_000L)
    public void stressTest() throws Exception {
        sqlService.execute(setupFetchingQuery());

        try (SqlResult result = sqlService.execute("SELECT * FROM " + sinkTopic)) {
            for (SqlRow sqlRow : result) {
                String s = sqlRow.getObject(1);
                resultSet.merge(s, 1, Integer::sum);
                if (resultSet.size() == expectedEventsCount) {
                    break;
                }
            }
        }

        Job job = instance().getJet().getJob(JOB_NAME);
        jobRestarter.finish();
        jobRestarter.join();
        assertNotNull(job);
        assertJobStatusEventually(job, RUNNING);
        job.cancel();
        assertJobStatusEventually(job, FAILED);

        if (processingGuarantee.equals(EXACTLY_ONCE) || restartGraceful) {
            List<Entry<String, Integer>> duplicates = resultSet.entrySet()
                    .stream()
                    .filter(entry -> entry.getValue() > 1)
                    .collect(Collectors.toList());

            assertThat(duplicates)
                    .as("Non-unique result count: " + duplicates.size())
                    .isEmpty();
        }

        for (int i = firstItemId; i <= lastItemId; ++i) {
            String key = "value-" + i;
            assertThat(resultSet.remove(key))
                    .as("Missing element: " + key)
                    .isNotNull();
        }

        assertThat(resultSet)
                .as("Unexpected items in the result: " + resultSet)
                .isEmpty();
    }

    protected String setupFetchingQuery() {
        return "CREATE JOB " + JOB_NAME +
                " OPTIONS ('processingGuarantee'='" + processingGuarantee + "', 'snapshotIntervalMillis' = '1000')" +
                " AS SINK INTO " + sinkTopic +
                " SELECT s1.__key, s2.this FROM s1 JOIN s2 ON s1.__key = s2.__key";
    }

    class JobRestarter extends Thread {
        private final JetService jetService;
        private final JetServiceBackend jetBackend;
        private volatile boolean finished;

        JobRestarter(HazelcastInstance hazelcastInstance) {
            this.jetService = hazelcastInstance.getJet();
            this.jetBackend = getJetServiceBackend(hazelcastInstance);
        }

        @Override
        public void run() {
            try {
                // wait for job start
                assertTrueEventually(() -> assertNotNull(jetService.getJob(JOB_NAME)));

                @SuppressWarnings("rawtypes")
                AbstractJobProxy job = (AbstractJobProxy) jetService.getJob(JOB_NAME);
                assertNotNull(job);

                Long lastExecutionId = null;
                while (!finished) {
                    waitForNextSnapshot(jetBackend.getJobRepository(), job.getId(), SNAPSHOT_TIMEOUT_SECONDS, true);
                    job.restart(restartGraceful);
                    lastExecutionId = assertJobRunningEventually(instance(), job, lastExecutionId);
                }
            } catch (NullPointerException e) {
                System.err.println(e);
            } catch (Throwable e) {
                logger.warning(null, e);
                ex = e;
            }
        }

        public void finish() {
            this.finished = true;
        }
    }

    private void createTopicData(SqlService sqlService, String topicName) {
        try {
            int itemsSank = 0;
            for (int sink = 1; sink <= sinkCount; sink++) {
                StringBuilder queryBuilder = new StringBuilder("INSERT INTO " + topicName + " VALUES ");
                for (int i = 0; i < eventsPerSink; ++i) {
                    ++itemsSank;
                    queryBuilder.append("(").append(itemsSank).append(", 'value-").append(itemsSank).append("'),");
                }
                queryBuilder.setLength(queryBuilder.length() - 1);

                assertEquals(itemsSank, eventsPerSink * sink);
                sqlService.execute(queryBuilder.toString());
                logger.info("Items sank " + itemsSank);
            }
        } catch (Throwable e) {
            logger.warning(null, e);
            ex = e;
        }
    }
}
