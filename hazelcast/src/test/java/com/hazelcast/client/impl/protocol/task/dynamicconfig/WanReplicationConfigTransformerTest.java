/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hazelcast.client.impl.protocol.task.dynamicconfig;

import com.hazelcast.client.impl.protocol.codec.holder.WanBatchPublisherConfigHolder;
import com.hazelcast.client.impl.protocol.codec.holder.WanConsumerConfigHolder;
import com.hazelcast.client.impl.protocol.codec.holder.WanCustomPublisherConfigHolder;
import com.hazelcast.config.AwsConfig;
import com.hazelcast.config.AzureConfig;
import com.hazelcast.config.ConsistencyCheckStrategy;
import com.hazelcast.config.DiscoveryConfig;
import com.hazelcast.config.DiscoveryStrategyConfig;
import com.hazelcast.config.EurekaConfig;
import com.hazelcast.config.GcpConfig;
import com.hazelcast.config.KubernetesConfig;
import com.hazelcast.config.WanAcknowledgeType;
import com.hazelcast.config.WanBatchPublisherConfig;
import com.hazelcast.config.WanConsumerConfig;
import com.hazelcast.config.WanCustomPublisherConfig;
import com.hazelcast.config.WanQueueFullBehavior;
import com.hazelcast.config.WanSyncConfig;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.wan.WanConsumer;
import com.hazelcast.wan.WanPublisher;
import com.hazelcast.wan.WanPublisherState;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.mockito.Mockito;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class WanReplicationConfigTransformerTest {
    private static final String TEST_CLASS_NAME = "com.hazelcast.Test";
    private SerializationService serializationService;
    private WanReplicationConfigTransformer transformer;

    @Before
    public void before() {
        serializationService = Mockito.mock(SerializationService.class);
        transformer = new WanReplicationConfigTransformer(serializationService);
    }

    private Map<String, Comparable> mockProperties() {
        String stringValue = "test";
        Data stringData = Mockito.mock(Data.class);
        Mockito.when(serializationService.toData(stringValue)).thenReturn(stringData);
        Mockito.when(serializationService.toObject(stringData)).thenReturn(stringValue);

        int intValue = 1;
        Data intData = Mockito.mock(Data.class);
        Mockito.when(serializationService.toData(intValue)).thenReturn(intData);
        Mockito.when(serializationService.toObject(intData)).thenReturn(intValue);

        Map<String, Comparable> properties = new HashMap<>();
        properties.put("a", stringValue);
        properties.put("b", intValue);
        return properties;
    }

    @Test
    public void testGetWanPublisherState() {
        assertEquals(WanPublisherState.REPLICATING, transformer.getWanPublisherState((byte) 0));
        assertEquals(WanPublisherState.PAUSED, transformer.getWanPublisherState((byte) 1));
        assertEquals(WanPublisherState.STOPPED, transformer.getWanPublisherState((byte) 2));
        assertEquals(WanBatchPublisherConfig.DEFAULT_INITIAL_PUBLISHER_STATE, transformer.getWanPublisherState((byte) 3));
    }

    @Test
    public void testGetWanQueueFullBehaviour() {
        assertEquals(WanQueueFullBehavior.DISCARD_AFTER_MUTATION, transformer.getWanQueueFullBehaviour(0));
        assertEquals(WanQueueFullBehavior.THROW_EXCEPTION, transformer.getWanQueueFullBehaviour(1));
        assertEquals(WanQueueFullBehavior.THROW_EXCEPTION_ONLY_IF_REPLICATION_ACTIVE, transformer.getWanQueueFullBehaviour(2));
        assertEquals(WanBatchPublisherConfig.DEFAULT_QUEUE_FULL_BEHAVIOUR, transformer.getWanQueueFullBehaviour(3));
    }

    @Test
    public void testGetWanAcknowledgeType() {
        assertEquals(WanAcknowledgeType.ACK_ON_RECEIPT, transformer.getWanAcknowledgeType(0));
        assertEquals(WanAcknowledgeType.ACK_ON_OPERATION_COMPLETE, transformer.getWanAcknowledgeType(1));
        assertEquals(WanBatchPublisherConfig.DEFAULT_ACKNOWLEDGE_TYPE, transformer.getWanAcknowledgeType(2));
    }

    @Test
    public void testWanConsumerConfigDefault() {
        WanConsumerConfig expected = new WanConsumerConfig();
        WanConsumerConfigHolder holder = transformer.toHolder(expected);
        WanConsumerConfig actual = transformer.toConfig(holder);
        assertEquals(expected, actual);
    }


    @Test
    public void testWanConsumerConfigCustomClassName() {
        testWanConsumerConfigCustom(false);
    }

    @Test
    public void testWanConsumerConfigCustomImplementation() {
        testWanConsumerConfigCustom(true);
    }

    private void testWanConsumerConfigCustom(boolean useImplementation) {
        WanConsumerConfig expected = new WanConsumerConfig();

        if (useImplementation) {
            Data wanConsumerData = Mockito.mock(Data.class);
            WanConsumer wanConsumer = Mockito.mock(WanConsumer.class);
            Mockito.when(serializationService.toData(wanConsumer)).thenReturn(wanConsumerData);
            Mockito.when(serializationService.toObject(wanConsumerData)).thenReturn(wanConsumer);
            expected.setImplementation(wanConsumer);
        } else {
            expected.setClassName(TEST_CLASS_NAME);
        }

        expected.setProperties(mockProperties());
        expected.setPersistWanReplicatedData(true);

        WanConsumerConfigHolder holder = transformer.toHolder(expected);
        WanConsumerConfig actual = transformer.toConfig(holder);
        assertEquals(expected, actual);
    }

    @Test
    public void testWanCustomPublisherDefault() {
        WanCustomPublisherConfig expected = new WanCustomPublisherConfig();
        WanCustomPublisherConfigHolder holder = transformer.toHolder(expected);
        WanCustomPublisherConfig actual = transformer.toConfig(holder);
        assertEquals(expected, actual); // equality is on the fields of interest
    }

    @Test
    public void testWanCustomPublisherConfigCustomClassName() {
        testWanCustomPublisherConfigCustom(false);
    }

    @Test
    public void testWanCustomPublisherConfigCustomImplementation() {
        testWanCustomPublisherConfigCustom(true);
    }

    private void testWanCustomPublisherConfigCustom(boolean useImplementation) {
        WanCustomPublisherConfig expected = new WanCustomPublisherConfig();
        expected.setPublisherId("my-publisher-id");

        if (useImplementation) {
            Data wanPublisherData = Mockito.mock(Data.class);
            WanPublisher wanPublisher = Mockito.mock(WanPublisher.class);
            Mockito.when(serializationService.toData(wanPublisher)).thenReturn(wanPublisherData);
            Mockito.when(serializationService.toObject(wanPublisherData)).thenReturn(wanPublisher);
            expected.setImplementation(wanPublisher);
        } else {
            expected.setClassName(TEST_CLASS_NAME);
        }
        expected.setProperties(mockProperties());
        WanCustomPublisherConfigHolder holder = transformer.toHolder(expected);
        WanCustomPublisherConfig actual = transformer.toConfig(holder);
        assertEquals(expected, actual);
    }

    @Test
    public void testWanBatchPublisherConfigDefault() {
        WanBatchPublisherConfig expected = new WanBatchPublisherConfig();
        WanBatchPublisherConfigHolder holder = transformer.toHolder(expected);
        WanBatchPublisherConfig actual = transformer.toConfig(holder);
        assertWanBatchPublisherEqual(expected, actual);
    }

    @Test
    public void testWanBatchPublisherConfigCustomClassName() {
        testWanBatchPublisherConfigCustom(false);
    }

    @Test
    public void testWanBatchPublisherConfigWithImplementation() {
        testWanBatchPublisherConfigCustom(true);
    }

    private static void assertWanBatchPublisherEqual(WanBatchPublisherConfig expected, WanBatchPublisherConfig actual) {
        assertEquals(expected, actual); // pub id, impl, classname + properties

        assertEquals(expected.getClusterName(), actual.getClusterName());
        assertEquals(expected.getInitialPublisherState(), actual.getInitialPublisherState());
        assertEquals(expected.getQueueCapacity(), actual.getQueueCapacity());
        assertEquals(expected.getBatchSize(), actual.getBatchSize());
        assertEquals(expected.getBatchMaxDelayMillis(), actual.getBatchMaxDelayMillis());
        assertEquals(expected.getResponseTimeoutMillis(), actual.getResponseTimeoutMillis());
        assertEquals(expected.getQueueFullBehavior(), actual.getQueueFullBehavior());
        assertEquals(expected.getAcknowledgeType(), actual.getAcknowledgeType());
        assertEquals(expected.getDiscoveryPeriodSeconds(), actual.getDiscoveryPeriodSeconds());
        assertEquals(expected.getMaxTargetEndpoints(), actual.getMaxTargetEndpoints());
        assertEquals(expected.getMaxConcurrentInvocations(), actual.getMaxConcurrentInvocations());
        assertEquals(expected.isUseEndpointPrivateAddress(), actual.isUseEndpointPrivateAddress());
        assertEquals(expected.getIdleMinParkNs(), actual.getIdleMinParkNs());
        assertEquals(expected.getIdleMaxParkNs(), actual.getIdleMaxParkNs());
        assertEquals(expected.getTargetEndpoints(), actual.getTargetEndpoints());
        assertEquals(expected.getEndpoint(), actual.getEndpoint());

        assertEquals(expected.getAwsConfig(), actual.getAwsConfig());
        assertEquals(expected.getGcpConfig(), actual.getGcpConfig());
        assertEquals(expected.getAzureConfig(), actual.getAzureConfig());
        assertEquals(expected.getKubernetesConfig(), actual.getKubernetesConfig());
        assertEquals(expected.getEurekaConfig(), actual.getEurekaConfig());

        assertEquals(expected.getDiscoveryConfig().getNodeFilterClass(), actual.getDiscoveryConfig().getNodeFilterClass());
        assertEquals(expected.getDiscoveryConfig().getNodeFilter(), actual.getDiscoveryConfig().getNodeFilter());
        assertEquals(expected.getDiscoveryConfig().getDiscoveryServiceProvider(), actual.getDiscoveryConfig().getDiscoveryServiceProvider());

        assertEquals(expected.getDiscoveryConfig(), actual.getDiscoveryConfig());
        assertEquals(expected.getSyncConfig(), actual.getSyncConfig());
    }

    private void testWanBatchPublisherConfigCustom(boolean useImplementation) {
        WanBatchPublisherConfig expected = new WanBatchPublisherConfig();
        expected.setClusterName(UUID.randomUUID().toString());
        expected.setPublisherId(UUID.randomUUID().toString());
        expected.setProperties(mockProperties());

        if (useImplementation) {
            Data wanPublisherData = Mockito.mock(Data.class);
            WanPublisher wanPublisher = Mockito.mock(WanPublisher.class);
            Mockito.when(serializationService.toData(wanPublisher)).thenReturn(wanPublisherData);
            Mockito.when(serializationService.toObject(wanPublisherData)).thenReturn(wanPublisher);
            expected.setImplementation(wanPublisher);
        } else {
            expected.setClassName(TEST_CLASS_NAME);
        }


        expected.setInitialPublisherState(WanPublisherState.PAUSED);
        expected.setEndpoint("my-endpoint");
        expected.setIdleMinParkNs(1_00L);
        expected.setIdleMaxParkNs(1_000L);
        expected.setUseEndpointPrivateAddress(true);
        expected.setMaxTargetEndpoints(10);
        expected.setAcknowledgeType(WanAcknowledgeType.ACK_ON_OPERATION_COMPLETE);
        expected.setQueueCapacity(10_000);
        expected.setBatchSize(50);
        expected.setBatchMaxDelayMillis(100);
        expected.setResponseTimeoutMillis(5_000);
        expected.setQueueFullBehavior(WanQueueFullBehavior.THROW_EXCEPTION_ONLY_IF_REPLICATION_ACTIVE);
        expected.setDiscoveryPeriodSeconds(1);
        expected.setMaxConcurrentInvocations(5);
        expected.setTargetEndpoints("a,b,c");
        expected.setSnapshotEnabled(true);

        Map<String, String> cloudProperties = new HashMap<>();
        cloudProperties.put("cloud-key", "cloud-value");
        AwsConfig awsConfig = new AwsConfig("aws-test", true, true, cloudProperties);
        GcpConfig gcpConfig = new GcpConfig("gcp-test", true, true, cloudProperties);
        AzureConfig azureConfig = new AzureConfig("azure-test", true, true, cloudProperties);
        KubernetesConfig k8sConfig = new KubernetesConfig("k8s-test", true, true, cloudProperties);
        EurekaConfig eurekaConfig = new EurekaConfig("eureka-test", true, true, cloudProperties);
        expected.setAwsConfig(awsConfig);
        expected.setGcpConfig(gcpConfig);
        expected.setAzureConfig(azureConfig);
        expected.setKubernetesConfig(k8sConfig);
        expected.setEurekaConfig(eurekaConfig);
        WanSyncConfig wanSyncConfig = new WanSyncConfig();
        wanSyncConfig.setConsistencyCheckStrategy(ConsistencyCheckStrategy.MERKLE_TREES);
        expected.setSyncConfig(wanSyncConfig);

        DiscoveryConfig discoveryConfig = new DiscoveryConfig();
        DiscoveryStrategyConfig discoveryStrategyConfig1 = new DiscoveryStrategyConfig(TEST_CLASS_NAME + "1");
        DiscoveryStrategyConfig discoveryStrategyConfig2 = new DiscoveryStrategyConfig(TEST_CLASS_NAME + "2");
        discoveryConfig.setDiscoveryStrategyConfigs(List.of(discoveryStrategyConfig1, discoveryStrategyConfig2));
        discoveryConfig.setNodeFilterClass(TEST_CLASS_NAME + "Filter");
        expected.setDiscoveryConfig(discoveryConfig);

        WanBatchPublisherConfigHolder holder = transformer.toHolder(expected);
        WanBatchPublisherConfig actual = transformer.toConfig(holder);

        assertWanBatchPublisherEqual(expected, actual);
    }
}
