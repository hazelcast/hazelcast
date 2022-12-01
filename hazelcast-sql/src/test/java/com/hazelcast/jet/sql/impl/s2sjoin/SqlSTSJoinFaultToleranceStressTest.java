package com.hazelcast.jet.sql.impl.s2sjoin;

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
    private static KafkaTestSupport kafkaTestSupport;

    private SqlService sqlService;
    private String mapName;
    private String topicName;
    private IMap<Integer, String> map;

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
    public static void afterClass() throws Exception {
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
        SqlResult result = sqlService.execute(query);
        assertEquals(0, result.updateCount());
        assertTrueEventually(() -> assertEquals(10000, map.size()));
    }

    private static String createRandomTopic() {
        String topicName = randomName();
        kafkaTestSupport.createTopic(topicName, INITIAL_PARTITION_COUNT);
        return topicName;
    }

    private static void createTopicData(SqlService sqlService, String topicName) {
        StringBuilder queryBuilder = new StringBuilder("INSERT INTO " + topicName + " VALUES ");
        int limit = 10000;
        for (int i = 1; i < limit; ++i) {
            queryBuilder.append("(").append(i).append(", 'value-").append(i).append("'), ");
        }
        queryBuilder.append("(").append(limit).append(", 'value-").append(limit).append("')");

        sqlService.execute(queryBuilder.toString());
    }
}
