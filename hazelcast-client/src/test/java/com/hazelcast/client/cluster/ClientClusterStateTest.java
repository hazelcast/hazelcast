package com.hazelcast.client.cluster;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.ClientProperties;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.cluster.ClusterState;
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.cluster.impl.AdvancedClusterStateTest.changeClusterStateEventually;
import static com.hazelcast.test.HazelcastTestSupport.assertClusterSizeEventually;
import static com.hazelcast.test.HazelcastTestSupport.randomMapName;
import static com.hazelcast.test.HazelcastTestSupport.warmUpPartitions;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class ClientClusterStateTest {

    private TestHazelcastFactory factory;

    private HazelcastInstance[] instances;

    private HazelcastInstance instance;

    @Before
    public void before() {
        factory = new TestHazelcastFactory();
        instances = factory.newInstances(new Config(), 3);
        for (HazelcastInstance instance : instances) {
            assertClusterSizeEventually(3, instance);
        }
        instance = instances[instances.length - 1];
    }

    @After
    public void after() {
        factory.terminateAll();
    }

    @Test
    public void testClient_canConnect_whenClusterState_frozen() {
        instance.getCluster().changeClusterState(ClusterState.FROZEN);
        factory.newHazelcastClient();
    }

    @Test
    public void testClient_canExecuteWriteOperations_whenClusterState_frozen() {
        warmUpPartitions(instances);

        changeClusterStateEventually(instance, ClusterState.FROZEN);
        final HazelcastInstance client = factory.newHazelcastClient();
        final IMap<Object, Object> map = client.getMap(randomMapName());
        map.put(1, 1);
    }

    @Test
    public void testClient_canExecuteReadOperations_whenClusterState_frozen() {
        warmUpPartitions(instances);

        changeClusterStateEventually(instance, ClusterState.FROZEN);
        final HazelcastInstance client = factory.newHazelcastClient();
        final IMap<Object, Object> map = client.getMap(randomMapName());
        map.get(1);
    }

    @Test
    public void testClient_canConnect_whenClusterState_passive() {
        instance.getCluster().changeClusterState(ClusterState.PASSIVE);
        factory.newHazelcastClient();
    }

    @Test(expected = IllegalStateException.class)
    public void testClient_canNotExecuteWriteOperations_whenClusterState_passive() {
        warmUpPartitions(instances);

        final ClientConfig clientConfig = new ClientConfig().setProperty(ClientProperties.PROP_INVOCATION_TIMEOUT_SECONDS, "3");
        final HazelcastInstance client = factory.newHazelcastClient(clientConfig);
        final IMap<Object, Object> map = client.getMap(randomMapName());
        changeClusterStateEventually(instance, ClusterState.PASSIVE);
        map.put(1, 1);
    }

    @Test
    public void testClient_canExecuteReadOperations_whenClusterState_passive() {
        warmUpPartitions(instances);

        final HazelcastInstance client = factory.newHazelcastClient();
        final IMap<Object, Object> map = client.getMap(randomMapName());
        changeClusterStateEventually(instance, ClusterState.PASSIVE);
        map.get(1);
    }

    @Test
    public void testClient_canConnect_whenClusterState_goesBackToActive_fromPassive() {
        instance.getCluster().changeClusterState(ClusterState.PASSIVE);
        instance.getCluster().changeClusterState(ClusterState.ACTIVE);
        factory.newHazelcastClient();
    }

    @Test
    public void testClient_canExecuteOperations_whenClusterState_goesBackToActive_fromPassive() {
        warmUpPartitions(instances);

        changeClusterStateEventually(instance, ClusterState.PASSIVE);
        instance.getCluster().changeClusterState(ClusterState.ACTIVE);
        final HazelcastInstance client = factory.newHazelcastClient();
        final IMap<Object, Object> map = client.getMap(randomMapName());
        map.put(1, 1);
    }

}
