package com.hazelcast.jet.sql.impl.misc;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.ClientSqlResubmissionMode;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.sql.SqlTestSupport;
import com.hazelcast.map.IMap;
import com.hazelcast.sql.HazelcastSqlException;
import com.hazelcast.sql.SqlResult;
import com.hazelcast.sql.SqlRow;
import com.hazelcast.sql.SqlStatement;
import com.hazelcast.test.HazelcastParametrizedRunner;
import com.hazelcast.test.HazelcastSerialParametersRunnerFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.UUID;

import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParametrizedRunner.class)
@Parameterized.UseParametersRunnerFactory(HazelcastSerialParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class SqlResubmissionTest extends SqlTestSupport {
    private static final int INITIAL_CLUSTER_SIZE = 1;
    private static final int MAP_SIZE = 10_000;
    private static final Config SMALL_INSTANCE_CONFIG = smallInstanceConfig();
    private static final String MAP_NAME = randomName();

    @Parameterized.Parameter
    public ClusterFailure clusterFailure;

    @Parameterized.Parameters(name = "clusterFailure:{0}")
    public static Collection<Object[]> parameters() {
        List<Object[]> res = new ArrayList<>();
        res.add(new Object[]{new NodeReplacementClusterFailure()});
        res.add(new Object[]{new NodeShutdownClusterFailure()});
        res.add(new Object[]{new NetworkProblemClusterFailure()});
        res.add(new Object[]{new NodeTerminationClusterFailure()});
        return res;
    }

    @Test
    public void when_allowsDuplicates() {
        clusterFailure.initialize();
        HazelcastInstance client = clusterFailure.createClient(new ClientConfig().setSqlResubmissionMode(ClientSqlResubmissionMode.RETRY_SELECTS_ALLOW_DUPLICATES));

        int count = 0;
        SqlStatement statement = new SqlStatement("select * from " + MAP_NAME);
        statement.setCursorBufferSize(1);
        SqlResult rows = client.getSql().execute(statement);
        for (SqlRow row : rows) {
            if (count == MAP_SIZE / 2) {
                logger.info("Half of the map is fetched, time to fail");
                clusterFailure.fail();
            }
            count++;
        }
        logger.info("Count: " + count);
        assertTrue(MAP_SIZE < count);

        clusterFailure.cleanUp();
    }

    @Test
    public void when_notAllowsDuplicates() {
        clusterFailure.initialize();
        HazelcastInstance client = clusterFailure.createClient(new ClientConfig().setSqlResubmissionMode(ClientSqlResubmissionMode.RETRY_SELECTS));

        SqlStatement statement = new SqlStatement("select * from " + MAP_NAME);
        statement.setCursorBufferSize(1);
        SqlResult rows = client.getSql().execute(statement);

        assertThrows(HazelcastSqlException.class, () -> {
            boolean firstRow = true;
            for (SqlRow row : rows) {
                if (firstRow) {
                    clusterFailure.fail();
                    firstRow = false;
                }
            }
        });

        clusterFailure.cleanUp();
    }

    @Test
    public void when_noResubmissionAllowed() {
        clusterFailure.initialize();
        HazelcastInstance client = clusterFailure.createClient(new ClientConfig().setSqlResubmissionMode(ClientSqlResubmissionMode.NEVER));

        SqlStatement statement = new SqlStatement("select * from " + MAP_NAME);
        statement.setCursorBufferSize(1);
        SqlResult rows = client.getSql().execute(statement);
        clusterFailure.fail();
        assertThrows(HazelcastSqlException.class, () -> {
            for (SqlRow row : rows) {
            }
        });
        clusterFailure.cleanUp();
    }

    private static class NetworkProblemClusterFailure extends SingleFailingInstanceClusterFailure {
        @Override
        public void fail() {
            for (HazelcastInstance hazelcastInstance : hazelcastInstances) {
                closeConnectionBetween(hazelcastInstance, failingInstance);
            }
            assertClusterSizeEventually(INITIAL_CLUSTER_SIZE, hazelcastInstances[0]);
        }

        @Override
        public void cleanUp() {
            super.cleanUp();
            failingInstance.shutdown();
        }
    }

    private static class NodeReplacementClusterFailure extends SingleFailingInstanceClusterFailure {
        private HazelcastInstance replacementInstance;

        @Override
        public void fail() {
            failingInstance.getLifecycleService().terminate();
            assertClusterSizeEventually(INITIAL_CLUSTER_SIZE, hazelcastInstances[0]);
            replacementInstance = factory.newHazelcastInstance(SMALL_INSTANCE_CONFIG);
            assertClusterSizeEventually(INITIAL_CLUSTER_SIZE + 1, hazelcastInstances[0]);
        }

        @Override
        public void cleanUp() {
            super.cleanUp();
            replacementInstance.shutdown();
        }
    }

    private static class NodeTerminationClusterFailure extends SingleFailingInstanceClusterFailure {
        @Override
        public void fail() {
            failingInstance.getLifecycleService().terminate();
            assertClusterSizeEventually(INITIAL_CLUSTER_SIZE, hazelcastInstances[0]);
        }
    }

    private static class NodeShutdownClusterFailure extends SingleFailingInstanceClusterFailure {
        @Override
        public void fail() {
            failingInstance.shutdown();
            assertClusterSizeEventually(INITIAL_CLUSTER_SIZE, hazelcastInstances[0]);
        }
    }

    private static abstract class SingleFailingInstanceClusterFailure implements ClusterFailure {
        protected HazelcastInstance[] hazelcastInstances;
        protected HazelcastInstance failingInstance;
        protected TestHazelcastFactory factory = new TestHazelcastFactory();
        protected HazelcastInstance client;

        @Override
        public void initialize() {
            hazelcastInstances = factory.newInstances(SMALL_INSTANCE_CONFIG, INITIAL_CLUSTER_SIZE);
            assertClusterSizeEventually(INITIAL_CLUSTER_SIZE, hazelcastInstances[0]);
            failingInstance = factory.newHazelcastInstance(SMALL_INSTANCE_CONFIG);
            assertClusterSizeEventually(INITIAL_CLUSTER_SIZE + 1, hazelcastInstances[0]);
            waitAllForSafeState(hazelcastInstances);
            waitAllForSafeState(failingInstance);
            createMap(hazelcastInstances[0]);
            client = null;
        }

        @Override
        public HazelcastInstance createClient(ClientConfig clientConfig) {
            client = factory.newHazelcastClient(clientConfig);
            return client;
        }

        @Override
        public void cleanUp() {
            Arrays.stream(hazelcastInstances).forEach(HazelcastInstance::shutdown);
            client.shutdown();
        }

        private void createMap(HazelcastInstance instance) {
            IMap<Long, UUID> map = instance.getMap(MAP_NAME);
            for (int i = 0; i < MAP_SIZE; i++) {
                map.put((long) i, UUID.randomUUID());
            }
            createMapping(instance, MAP_NAME, Long.class, UUID.class);
        }
    }

    private interface ClusterFailure {
        void initialize();

        void fail();

        void cleanUp();

        HazelcastInstance createClient(ClientConfig clientConfig);
    }
}
