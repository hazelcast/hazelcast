package com.hazelcast.jet.sql.impl.s2sjoin;

import com.hazelcast.jet.JetService;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.kafka.impl.KafkaTestSupport;
import com.hazelcast.jet.sql.SqlTestSupport;
import com.hazelcast.jet.sql.impl.connector.kafka.KafkaSqlConnector;
import com.hazelcast.map.IMap;
import com.hazelcast.sql.SqlResult;
import com.hazelcast.sql.SqlService;
import com.hazelcast.test.HazelcastParametrizedRunner;
import com.hazelcast.test.HazelcastSerialParametersRunnerFactory;
import com.hazelcast.test.annotation.NightlyTest;
import com.hazelcast.test.annotation.ParallelJVMTest;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import java.io.IOException;

import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_KEY_FORMAT;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_VALUE_FORMAT;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParametrizedRunner.class)
@UseParametersRunnerFactory(HazelcastSerialParametersRunnerFactory.class)
@Category({NightlyTest.class, ParallelJVMTest.class})
public class SqlSTSJoinFaultToleranceStressTest extends SqlTestSupport {
    private static final int INITIAL_PARTITION_COUNT = 1;
    private static final int EVENTS_COUNT = 20_000;
    private static KafkaTestSupport kafkaTestSupport;

    private SqlService sqlService;
    private String mapName;
    private String topicName;
    private IMap<Integer, String> map;
    private AssertionError ex;

    private String query;

    @Parameter
    public String processingGuarantee;

    @Parameters(name = "{0}")
    public static Iterable<Object> parameters() {
        return asList("none", "atLeastOnce", "exactlyOnce");
    }

    @BeforeClass
    public static void beforeClass() throws IOException {
        initialize(3, null);
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
        mapName = randomName();
        createMapping(mapName, Integer.class, String.class);
        map = instance().getMap(mapName);

        topicName = createRandomTopic();
        sqlService.execute("CREATE MAPPING " + topicName + ' '
                + "TYPE " + KafkaSqlConnector.TYPE_NAME + ' '
                + "OPTIONS ( "
                + '\'' + OPTION_KEY_FORMAT + "'='int'"
                + ", '" + OPTION_VALUE_FORMAT + "'='varchar'"
                + ", 'bootstrap.servers'='" + kafkaTestSupport.getBrokerConnectionString() + '\''
                + ", 'auto.offset.reset'='earliest'"
                + ")");

        createTopicData(sqlService, topicName);

        sqlService.execute("CREATE VIEW s1 AS " +
                "SELECT * FROM TABLE(IMPOSE_ORDER(TABLE " + topicName + " , DESCRIPTOR(__key), 10))");
        sqlService.execute("CREATE VIEW s2 AS " +
                "SELECT * FROM TABLE(IMPOSE_ORDER(TABLE " + topicName + " , DESCRIPTOR(__key), 5))");

        query = "CREATE JOB job OPTIONS ('processingGuarantee'='" + processingGuarantee + "') AS " +
                " SINK INTO " + mapName +
                " SELECT s1.__key, s2.this FROM s1 JOIN s2 ON s1.__key = s2.__key";
    }

    @Test
    public void stressTest() {
        JobRestarter jobRestarter = new JobRestarter(instance().getJet());
        jobRestarter.start();

        SqlResult result = sqlService.execute(query);
        assertEquals(0, result.updateCount());
        assertTrueEventually(() -> assertEquals(EVENTS_COUNT, map.size()));
        jobRestarter.finish();

        try {
            jobRestarter.join();
        } catch (AssertionError e) {
            Assert.fail(e.getMessage());
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    static class JobRestarter extends Thread {
        private final JetService jetService;
        private volatile boolean finish;

        JobRestarter(JetService jetService) {
            this.jetService = jetService;
        }

        @Override
        public void run() {
            for (; ; ) {
                try {
                    if (finish) {
                        return;
                    }
                    Thread.sleep(1000L);
                    assert jetService.getJobs().size() == 1;
                    Job job = jetService.getJobs().get(0);
                    job.restart();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        }

        public void finish() {
            this.finish = true;
        }
    }

    private static String createRandomTopic() {
        String topicName = randomName();
        kafkaTestSupport.createTopic(topicName, INITIAL_PARTITION_COUNT);
        return topicName;
    }

    private static void createTopicData(SqlService sqlService, String topicName) {
        StringBuilder queryBuilder = new StringBuilder("INSERT INTO " + topicName + " VALUES ");
        for (int i = 1; i < EVENTS_COUNT; ++i) {
            queryBuilder.append("(").append(i).append(", 'value-").append(i).append("'), ");
        }
        queryBuilder.append("(").append(EVENTS_COUNT).append(", 'value-").append(EVENTS_COUNT).append("')");

        sqlService.execute(queryBuilder.toString());
    }
}
