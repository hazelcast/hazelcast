package com.hazelcast.jclouds;

import com.hazelcast.core.HazelcastException;
import com.hazelcast.nio.Address;
import com.hazelcast.spi.discovery.DiscoveryNode;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.jclouds.compute.domain.NodeMetadata;
import org.jclouds.compute.domain.internal.NodeMetadataImpl;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class JCloudsDiscoveryStrategyTest extends HazelcastTestSupport {

    private static final int STARTING_PORT = 5701;
    private static final int NUMBER_OF_NODES = 10;
    private static final int NUMBER_OF_RUNNING_NODES = 7;

    private Set<NodeMetadata> nodes = new HashSet<NodeMetadata>();
    private Set<Address> addressesOfRunningInstances = new HashSet<Address>();

    @Before
    public void setup()
            throws UnknownHostException {
        for (int i = 0; i < NUMBER_OF_NODES; i++) {
            HashSet<String> privateAdresses = new HashSet<String>();
            privateAdresses.add("127.0.0." + (i+1));
            NodeMetadata.Status status = NodeMetadata.Status.PENDING;
            if (i < NUMBER_OF_RUNNING_NODES) {
                status = NodeMetadata.Status.RUNNING;
                addressesOfRunningInstances.add(new Address(privateAdresses.iterator().next(),0));
            }
            nodes.add(new NodeMetadataImpl("", "", "dummyId"+i, null, null, new HashMap<String, String>(), new HashSet<String>(), null, null, null, null, status, "",
                    STARTING_PORT + i, privateAdresses, privateAdresses, null, "dummyHostName" + i));
        }
    }

    @Test
    public void testShouldDiscoverOnlyRunningNodes() {
        ComputeServiceBuilder mockComputeServiceBuilder = mock(ComputeServiceBuilder.class);
        doReturn(nodes).when(mockComputeServiceBuilder).getFilteredNodes();

        JCloudsDiscoveryStrategy jCloudsDiscoveryStrategy = new JCloudsDiscoveryStrategy(mockComputeServiceBuilder);

        Iterable<DiscoveryNode> runningNodes = jCloudsDiscoveryStrategy.discoverNodes();

        for (DiscoveryNode node : runningNodes) {
            assertTrue(addressesOfRunningInstances.contains(node.getPrivateAddress()));
        }

    }

    @Test
    public void testBuildCalled() {
        ComputeServiceBuilder mockComputeServiceBuilder = mock(ComputeServiceBuilder.class);
        JCloudsDiscoveryStrategy jCloudsDiscoveryStrategy = new JCloudsDiscoveryStrategy(mockComputeServiceBuilder);
        jCloudsDiscoveryStrategy.start();

        verify(mockComputeServiceBuilder).build();
    }

    @Test
    public void testDestroyCalled() {
        ComputeServiceBuilder mockComputeServiceBuilder = mock(ComputeServiceBuilder.class);
        JCloudsDiscoveryStrategy jCloudsDiscoveryStrategy = new JCloudsDiscoveryStrategy(mockComputeServiceBuilder);
        jCloudsDiscoveryStrategy.destroy();

        verify(mockComputeServiceBuilder).destroy();
    }

    @Test(expected = HazelcastException.class)
    public void whenInvalidAddress_thenHazelcastException() {
        HashSet<String> privateAdressses = new HashSet<String>();
        //invalid address
        privateAdressses.add("257.0.0.1");
        Set<NodeMetadata> nodes = new HashSet<NodeMetadata>();
        nodes.add(new NodeMetadataImpl("", "", "dummyId", null, null, new HashMap<String, String>(), new HashSet<String>(), null, null, null, null,
                NodeMetadata.Status.RUNNING, "",
                STARTING_PORT, privateAdressses, privateAdressses, null, "dummyHostName"));
        ComputeServiceBuilder mockComputeServiceBuilder = mock(ComputeServiceBuilder.class);
        doReturn(nodes).when(mockComputeServiceBuilder).getFilteredNodes();

        JCloudsDiscoveryStrategy jCloudsDiscoveryStrategy = new JCloudsDiscoveryStrategy(mockComputeServiceBuilder);
        jCloudsDiscoveryStrategy.discoverNodes();
    }
}
