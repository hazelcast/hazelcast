package com.hazelcast.jclouds;

import com.google.common.base.Predicate;
import com.hazelcast.config.NetworkConfig;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.util.UuidUtil;
import org.jclouds.aws.ec2.compute.AWSEC2ComputeService;
import org.jclouds.compute.ComputeService;
import org.jclouds.compute.ComputeServiceContext;
import org.jclouds.compute.domain.ComputeMetadata;
import org.jclouds.compute.domain.NodeMetadata;
import org.jclouds.compute.domain.NodeMetadataBuilder;
import org.jclouds.domain.Location;
import org.jclouds.domain.LocationBuilder;
import org.jclouds.domain.LocationScope;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.net.URL;
import java.net.URLDecoder;
import java.util.AbstractMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class ComputeServiceBuilderTest extends HazelcastTestSupport {

    @Test
    public void test_getProperties() throws Exception {
        Map<String,Comparable> properties = new HashMap<String, Comparable>();
        properties.put("key", "value");
        ComputeServiceBuilder builder = new ComputeServiceBuilder(properties);
        assertEquals(1, builder.getProperties().size());
        assertEquals("value", builder.getProperties().get("key"));
    }
    
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
        assertNotEquals(builder.getServicePort(), NetworkConfig.DEFAULT_PORT);
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
        for (NodeMetadata node : result) {
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


    @Test
    public void test_buildZonesConfig_parses_multiple_zones() throws Exception {
        Map<String, Comparable> properties = new HashMap<String, Comparable>();
        properties.put("zones", "zone1,zone2,zone3");
        ComputeServiceBuilder builder = new ComputeServiceBuilder(properties);
        builder.buildRegionZonesConfig();
        assertEquals(3, builder.getZonesSet().size());
        assertTrue(builder.getZonesSet().contains("zone1"));
        assertTrue(builder.getZonesSet().contains("zone2"));
        assertTrue(builder.getZonesSet().contains("zone3"));
    }

    @Test
    public void test_buildZonesConfig_parse_single_zone() throws Exception {
        Map<String, Comparable> properties = new HashMap<String, Comparable>();
        properties.put("zones", "zone1");
        ComputeServiceBuilder builder = new ComputeServiceBuilder(properties);
        builder.buildRegionZonesConfig();
        assertEquals(1, builder.getZonesSet().size());
        assertTrue(builder.getZonesSet().contains("zone1"));
    }

    @Test
    public void test_buildZonesConfig_parse_multiple_regions() throws Exception {
        Map<String, Comparable> properties = new HashMap<String, Comparable>();
        properties.put("regions", "region1,region2,region3");
        ComputeServiceBuilder builder = new ComputeServiceBuilder(properties);
        builder.buildRegionZonesConfig();
        assertEquals(3, builder.getRegionsSet().size());
        assertTrue(builder.getRegionsSet().contains("region1"));
        assertTrue(builder.getRegionsSet().contains("region2"));
        assertTrue(builder.getRegionsSet().contains("region3"));
    }

    @Test
    public void test_buildZonesConfig_parse_single_region() throws Exception {
        Map<String, Comparable> properties = new HashMap<String, Comparable>();
        properties.put("regions", "region1");
        ComputeServiceBuilder builder = new ComputeServiceBuilder(properties);
        builder.buildRegionZonesConfig();
        assertEquals(1, builder.getRegionsSet().size());
        assertTrue(builder.getRegionsSet().contains("region1"));
    }

    @Test
    public void test_isNodeInsideZones_when_zone_set_is_empty() throws Exception {
        ComputeServiceBuilder builder = new ComputeServiceBuilder(new HashMap<String, Comparable>());
        builder.buildRegionZonesConfig();

        assertTrue(builder.isNodeInsideZones(
                getRunningNodeMetaDataAtLocation(LocationScope.ZONE, "testZone")));

    }

    @Test
    public void test_isNodeInsideZones_when_node_is_not_in_zone_set() throws Exception {
        Map<String, Comparable> properties = new HashMap<String, Comparable>();
        properties.put("zones", "zone1,zone2");
        ComputeServiceBuilder builder = new ComputeServiceBuilder(properties);
        builder.buildRegionZonesConfig();

        assertFalse(builder.isNodeInsideZones(
                getRunningNodeMetaDataAtLocation(LocationScope.ZONE, "testZone")));

    }

    @Test
    public void test_isNodeInsideZones_when_node_is_in_zone_set() throws Exception {
        Map<String, Comparable> properties = new HashMap<String, Comparable>();
        properties.put("zones", "zone1,testZone");
        ComputeServiceBuilder builder = new ComputeServiceBuilder(properties);
        builder.buildRegionZonesConfig();

        assertTrue(builder.isNodeInsideZones(
                getRunningNodeMetaDataAtLocation(LocationScope.ZONE, "testZone")));

    }

    @Test
    public void test_isNodeInsideRegions_when_region_set_is_empty() throws Exception {
        ComputeServiceBuilder builder = new ComputeServiceBuilder(new HashMap<String, Comparable>());
        builder.buildRegionZonesConfig();

        assertTrue(builder.isNodeInsideRegions(
                getRunningNodeMetaDataAtLocation(LocationScope.REGION, "testRegion")));

    }

    @Test
    public void test_isNodeInsideRegions_when_node_is_not_in_region_set() throws Exception {
        Map<String, Comparable> properties = new HashMap<String, Comparable>();
        properties.put("regions", "region1,region2");
        ComputeServiceBuilder builder = new ComputeServiceBuilder(properties);
        builder.buildRegionZonesConfig();

        assertFalse(builder.isNodeInsideRegions(
                getRunningNodeMetaDataAtLocation(LocationScope.REGION, "testRegion")));

    }

    @Test
    public void test_isNodeInsideRegions_when_node_is__in_region_set() throws Exception {
        Map<String, Comparable> properties = new HashMap<String, Comparable>();
        properties.put("regions", "region1,testRegion");
        ComputeServiceBuilder builder = new ComputeServiceBuilder(properties);
        builder.buildRegionZonesConfig();

        assertTrue(builder.isNodeInsideRegions(
                getRunningNodeMetaDataAtLocation(LocationScope.REGION, "testRegion")));

    }

    private NodeMetadata getRunningNodeMetaDataAtLocation(LocationScope scope, String id) {
        Location location = new LocationBuilder().scope(scope).id(id).description("desc").build();
        return new NodeMetadataBuilder().location(location).id(UuidUtil.newSecureUuidString()).
                status(NodeMetadata.Status.RUNNING).build();
    }

    @Test
    public void test_buildTag_Config_when_no_tags_configured() {
        ComputeServiceBuilder builder = new ComputeServiceBuilder(new HashMap<String, Comparable>());
        builder.buildTagConfig();

        assertTrue(builder.getTagPairs().isEmpty());
    }

    @Test
    public void test_buildTag_Config_when__tags_configured() {
        Map<String, Comparable> properties = new HashMap<String, Comparable>();
        properties.put("tag-keys", "tag1,tag2");
        properties.put("tag-values", "value1,value2");
        ComputeServiceBuilder builder = new ComputeServiceBuilder(properties);
        builder.buildTagConfig();

        assertEquals(2, builder.getTagPairs().size());
        assertTrue(builder.getTagPairs().contains(new AbstractMap.SimpleImmutableEntry("tag1", "value1")));
        assertTrue(builder.getTagPairs().contains(new AbstractMap.SimpleImmutableEntry("tag2", "value2")));
        assertFalse(builder.getTagPairs().contains(new AbstractMap.SimpleImmutableEntry("tag1", "value2")));
    }

    @Test
    public void test_buildNodeFilter_with_null_NodeMetadata() throws Exception {
        ComputeServiceBuilder builder = new ComputeServiceBuilder(new HashMap<String, Comparable>());
        Predicate<ComputeMetadata> nodeFilter = builder.buildNodeFilter();
        assertFalse(nodeFilter.apply(null));
    }

    @Test
    public void test_buildNodeFilter_with_NodeMetadata_with_multiple_tags() throws Exception {
        Map<String, Comparable> properties = new HashMap<String, Comparable>();
        properties.put("tag-keys", "tag1,tag2");
        properties.put("tag-values", "value1,value2");
        ComputeServiceBuilder builder = new ComputeServiceBuilder(properties);
        builder.buildTagConfig();
        Predicate<ComputeMetadata> nodeFilter = builder.buildNodeFilter();

        Map<String, String> userMetaData = new HashMap<String, String>();
        userMetaData.put("tag1", "value1");

        NodeMetadata metadata = new NodeMetadataBuilder().
                userMetadata(userMetaData).id(UuidUtil.newSecureUuidString()).
                status(NodeMetadata.Status.RUNNING).build();

        assertFalse(nodeFilter.apply(metadata));

    }

    @Test
    public void test_buildNodeFilter_with_NodeMetadata_with_single_tag() throws Exception {
        Map<String, Comparable> properties = new HashMap<String, Comparable>();
        properties.put("tag-keys", "tag1");
        properties.put("tag-values", "value1");
        ComputeServiceBuilder builder = new ComputeServiceBuilder(properties);
        builder.buildTagConfig();
        Predicate<ComputeMetadata> nodeFilter = builder.buildNodeFilter();

        Map<String, String> userMetaData = new HashMap<String, String>();
        userMetaData.put("tag1", "value2");

        NodeMetadata metadata = new NodeMetadataBuilder().
                userMetadata(userMetaData).id(UuidUtil.newSecureUuidString()).
                status(NodeMetadata.Status.RUNNING).build();

        assertFalse(nodeFilter.apply(metadata));

    }

    @Test
    public void test_buildNodeFilter_with_NodeMetadata_with_multiple_tags_with_and_relation() throws Exception {
        Map<String, Comparable> properties = new HashMap<String, Comparable>();
        properties.put("tag-keys", "Owner,Stack");
        properties.put("tag-values", "DbAdmin,Production");
        ComputeServiceBuilder builder = new ComputeServiceBuilder(properties);
        builder.buildTagConfig();
        Predicate<ComputeMetadata> nodeFilter = builder.buildNodeFilter();

        Map<String, String> userMetaData = new HashMap<String, String>();
        userMetaData.put("Owner", "DbAdmin");
        userMetaData.put("Stack", "Production");

        NodeMetadata metadata = new NodeMetadataBuilder().
                userMetadata(userMetaData).id(UuidUtil.newSecureUuidString()).
                status(NodeMetadata.Status.RUNNING).build();

        assertTrue(nodeFilter.apply(metadata));

    }

    @Test
    public void test_buildNodeFilter_with_NodeMetadata_with_multiple_tags_with_and_relation_where_user_metadata_has_more_elements() throws Exception {
        Map<String, Comparable> properties = new HashMap<String, Comparable>();
        properties.put("tag-keys", "Owner,Stack");
        properties.put("tag-values", "DbAdmin,Production");
        ComputeServiceBuilder builder = new ComputeServiceBuilder(properties);
        builder.buildTagConfig();
        Predicate<ComputeMetadata> nodeFilter = builder.buildNodeFilter();

        Map<String, String> userMetaData = new HashMap<String, String>();
        userMetaData.put("Owner", "DbAdmin");
        userMetaData.put("Stack", "Production");
        userMetaData.put("Number", "1");

        NodeMetadata metadata = new NodeMetadataBuilder().
                userMetadata(userMetaData).id(UuidUtil.newSecureUuidString()).
                status(NodeMetadata.Status.RUNNING).build();

        assertTrue(nodeFilter.apply(metadata));

    }

    @Test
    public void test_buildNodeFilter_with_NodeMetadata_with_single_value() throws Exception {
        Map<String, Comparable> properties = new HashMap<String, Comparable>();
        properties.put("tag-keys", "tag1");
        properties.put("tag-values", "value1");
        ComputeServiceBuilder builder = new ComputeServiceBuilder(properties);
        builder.buildTagConfig();
        Predicate<ComputeMetadata> nodeFilter = builder.buildNodeFilter();

        Map<String, String> userMetaData = new HashMap<String, String>();
        userMetaData.put("tag1", "value1");

        NodeMetadata metadata = new NodeMetadataBuilder().
                userMetadata(userMetaData).id(UuidUtil.newSecureUuidString()).
                status(NodeMetadata.Status.RUNNING).build();

        assertTrue(nodeFilter.apply(metadata));

    }

    @Test
    public void test_getCredentialFromFile_when_google_compute_engine() throws Exception {
        ComputeServiceBuilder builder = new ComputeServiceBuilder(new HashMap<String, Comparable>());
        URL resourceUrl = getClass().
                getResource("/google-json-credential.json");
        String decodedURL = URLDecoder.decode(resourceUrl.getFile(), "UTF-8");
        assertEquals("key", builder.getCredentialFromFile("google-compute-engine",
                decodedURL));
    }

    @Test
    public void test_getCredentialFromFile_when_cloud_provider_other_than_google() throws Exception {
        ComputeServiceBuilder builder = new ComputeServiceBuilder(new HashMap<String, Comparable>());
        URL resourceUrl = getClass().
                getResource("/key.properties");
        String decodedURL = URLDecoder.decode(resourceUrl.getFile(), "UTF-8");
        assertEquals("cloudkey", builder.getCredentialFromFile("gogrid",
                decodedURL));
    }

    @Test
    public void test_compute_service_builder_return_correct_api_class() throws Exception {
        Map<String, Comparable> properties = new HashMap<String, Comparable>();
        properties.put("provider", "aws-ec2");
        properties.put("identity", "id");
        properties.put("credential", "credential");
        ComputeServiceBuilder builder = new ComputeServiceBuilder(properties);
        ComputeService service = builder.build();
        assertEquals(AWSEC2ComputeService.class, service.getClass());
    }
}
