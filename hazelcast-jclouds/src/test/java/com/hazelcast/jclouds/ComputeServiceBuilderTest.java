package com.hazelcast.jclouds;

import com.hazelcast.config.NetworkConfig;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.util.UuidUtil;
import org.jclouds.compute.ComputeService;
import org.jclouds.compute.ComputeServiceContext;
import org.jclouds.compute.domain.NodeMetadata;
import org.jclouds.compute.domain.NodeMetadataBuilder;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class ComputeServiceBuilderTest extends HazelcastTestSupport {

    @Test
    public void test_getServicePort_returns_default_hz_port() throws Exception {
        ComputeServiceBuilder builder = new ComputeServiceBuilder(new HashMap<String, Comparable>());
        assertEquals(builder.getServicePort(), NetworkConfig.DEFAULT_PORT);
    }

    @Test
    public void test_getServicePort_returns_configured_hz_port() throws Exception {
        Map<String, Comparable> properties = new HashMap<String, Comparable>();
        properties.put("hz-port", 5703);
        ComputeServiceBuilder builder = new ComputeServiceBuilder(properties);
        assertEquals(builder.getServicePort(), 5703);
        assertNotEquals(builder.getServicePort(),NetworkConfig.DEFAULT_PORT);
    }

    @Test
    public void test_getFilteredNodes_with_group_configured() throws Exception {
        Map<String, Comparable> properties = new HashMap<String, Comparable>();
        String groupUnderTest = "group3";
        properties.put("group", groupUnderTest);
        ComputeServiceBuilder builder = new ComputeServiceBuilder(properties);
        ComputeService mockComputeService = mock(ComputeService.class);
        builder.setComputeService(mockComputeService);

        Set<NodeMetadata> nodes = new HashSet<NodeMetadata>();
        for (int i = 0; i < 5; i++) {
            nodes.add(new NodeMetadataBuilder().id(UuidUtil.newSecureUuidString()).group("group" + i)
                    .status(NodeMetadata.Status.RUNNING).build());
        }
        nodes.add(new NodeMetadataBuilder().id(UuidUtil.newSecureUuidString()).group(groupUnderTest)
                .status(NodeMetadata.Status.RUNNING).build());
        doReturn(nodes).when(mockComputeService).listNodesDetailsMatching(null);

        Set<NodeMetadata> result = (Set) builder.getFilteredNodes();

        assertEquals(2, result.size());
        for(NodeMetadata node : result){
            assertEquals(groupUnderTest, node.getGroup());
        }
    }

    @Test
    public void test_destroy_calls_computeService_destroy() throws Exception {
        ComputeServiceBuilder builder = new ComputeServiceBuilder(new HashMap<String, Comparable>());
        ComputeService mockComputeService = mock(ComputeService.class);
        ComputeServiceContext mockComputeServiceContext = mock(ComputeServiceContext.class);
        builder.setComputeService(mockComputeService);
        doReturn(mockComputeServiceContext).when(mockComputeService).getContext();
        builder.destroy();
        verify(mockComputeServiceContext).close();
    }
}
