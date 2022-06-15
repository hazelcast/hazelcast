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
import com.hazelcast.test.annotation.SlowTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.function.Supplier;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParametrizedRunner.class)
@Parameterized.UseParametersRunnerFactory(HazelcastSerialParametersRunnerFactory.class)
@Category({SlowTest.class, ParallelJVMTest.class})
public class SqlResubmissionTest extends SqlTestSupport {
    private static final int INITIAL_CLUSTER_SIZE = 1;
    private static final int COMMON_MAP_SIZE = 10_000;
    private static final int SLOW_MAP_SIZE = 10;
    private static final Config SMALL_INSTANCE_CONFIG = smallInstanceConfig();
    private static final String COMMON_MAP_NAME = randomName();
    private static final String SLOW_MAP_NAME = randomName();

    @Parameterized.Parameter(0)
    public ClusterFailure clusterFailure;

    @Parameterized.Parameter(1)
    public ClientSqlResubmissionMode resubmissionMode;

    @Parameterized.Parameters(name = "clusterFailure:{0}, resubmissionMode:{1}")
    public static Collection<Object[]> parameters() {
        List<Object[]> res = new ArrayList<>();
        List<SingleFailingInstanceClusterFailure> failures = Arrays.asList(
                new NodeReplacementClusterFailure(),
                new NodeShutdownClusterFailure(),
                new NetworkProblemClusterFailure(),
                new NodeTerminationClusterFailure()
        );
        for (ClientSqlResubmissionMode mode : ClientSqlResubmissionMode.values()) {
            for (SingleFailingInstanceClusterFailure failure : failures) {
                res.add(new Object[]{failure, mode});
            }
        }
        return res;
    }

    @Test
    public void when_failingSelectAfterSomeDataIsFetched() {
        clusterFailure.initialize();
        HazelcastInstance client = clusterFailure.createClient(new ClientConfig().setSqlResubmissionMode(resubmissionMode));

        SqlStatement statement = new SqlStatement("select * from " + COMMON_MAP_NAME);
        statement.setCursorBufferSize(1);
        SqlResult rows = client.getSql().execute(statement);

        try {
            if (shouldFailAfterSomeDataIsFetched(resubmissionMode)) {
                assertThrows(HazelcastSqlException.class, () -> {
                    countWithFailureInTheMiddle(rows);
                });
            } else {
                int count = countWithFailureInTheMiddle(rows);
                logger.info("Count: " + count);
                assertTrue(COMMON_MAP_SIZE < count);
            }
        } finally {
            clusterFailure.cleanUp();
        }
    }

    private boolean shouldFailAfterSomeDataIsFetched(ClientSqlResubmissionMode mode) {
        return mode == ClientSqlResubmissionMode.NEVER || mode == ClientSqlResubmissionMode.RETRY_SELECTS;
    }

    private int countWithFailureInTheMiddle(SqlResult rows) {
        int count = 0;
        for (SqlRow row : rows) {
            if (count == COMMON_MAP_SIZE / 2) {
                logger.info("Half of the map is fetched, time to fail");
                clusterFailure.fail();
            }
            count++;
        }
        return count;
    }

    @Test
    public void when_failingSelectBeforeAnyDataIsFetched() throws InterruptedException {
        clusterFailure.initialize();
        HazelcastInstance client = clusterFailure.createClient(new ClientConfig().setSqlResubmissionMode(resubmissionMode));
        SqlStatement statement = new SqlStatement("select field from " + SLOW_MAP_NAME);
        statement.setCursorBufferSize(SLOW_MAP_SIZE);
        Thread failingThread = new Thread(() -> {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
            }
            clusterFailure.fail();
        });
        failingThread.start();
        try {
            if (shouldFailBeforeAnyDataIsFetched(resubmissionMode)) {
                assertThrows(HazelcastSqlException.class, () -> client.getSql().execute(statement));
            } else {
                assertEquals(SLOW_MAP_SIZE, count(client.getSql().execute(statement)));
            }
        } finally {
            failingThread.join();
            clusterFailure.cleanUp();
        }
    }

    private boolean shouldFailBeforeAnyDataIsFetched(ClientSqlResubmissionMode mode) {
        return mode == ClientSqlResubmissionMode.NEVER;
    }

    private int count(SqlResult rows) {
        int count = 0;
        for (SqlRow row : rows) {
            count++;
        }
        return count;
    }

    @Test
    public void when_failingUpdate() throws InterruptedException {
        clusterFailure.initialize();
        HazelcastInstance client = clusterFailure.createClient(new ClientConfig().setSqlResubmissionMode(resubmissionMode));
        SqlStatement statement = new SqlStatement("update " + SLOW_MAP_NAME + " set field = field + 1");

        Thread failingThread = new Thread(() -> {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
            }
            clusterFailure.fail();
        });
        failingThread.start();
        try {
            if (shouldFailModifyingQuery(resubmissionMode)) {
                assertThrows(HazelcastSqlException.class, () -> client.getSql().execute(statement));
            } else {
                assertEquals(0, client.getSql().execute(statement).updateCount());
            }
        } finally {
            failingThread.join();
            clusterFailure.cleanUp();
        }
    }

    private boolean shouldFailModifyingQuery(ClientSqlResubmissionMode mode) {
        return mode != ClientSqlResubmissionMode.RETRY_ALL;
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
            failingInstance.getLifecycleService().terminate();
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
            replacementInstance.getLifecycleService().terminate();
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
            createMap(hazelcastInstances[0], COMMON_MAP_NAME, COMMON_MAP_SIZE, UUID::randomUUID, UUID.class);
            createMap(failingInstance, SLOW_MAP_NAME, SLOW_MAP_SIZE, SlowFieldAccessObject::new, SlowFieldAccessObject.class);
            client = null;
        }

        @Override
        public HazelcastInstance createClient(ClientConfig clientConfig) {
            client = factory.newHazelcastClient(clientConfig);
            return client;
        }

        @Override
        public void cleanUp() {
            Arrays.stream(hazelcastInstances).forEach(instance -> instance.getLifecycleService().terminate());
            client.getLifecycleService().terminate();
        }

        private <T> void createMap(HazelcastInstance instance, String name, int size, Supplier<T> objectCreator, Class<T> tClass) {
            IMap<Long, T> map = instance.getMap(name);
            for (int i = 0; i < size; i++) {
                map.put((long) i, objectCreator.get());
            }
            createMapping(instance, name, Long.class, tClass);
        }

        @Override
        public String toString() {
            return this.getClass().getSimpleName();
        }
    }

    private interface ClusterFailure {
        void initialize();

        void fail();

        void cleanUp();

        HazelcastInstance createClient(ClientConfig clientConfig);
    }

    public static class SlowFieldAccessObject implements Serializable {
        private int field = 0;

        public int getField() {
            try {
                Thread.sleep(2_000);
            } catch (InterruptedException e) {
            }
            return field;
        }

        public void setField(int field) {
            this.field = field;
        }
    }
}
