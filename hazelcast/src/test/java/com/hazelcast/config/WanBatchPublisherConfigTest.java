/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.config;

import com.hazelcast.config.ConfigCompatibilityChecker.WanBatchPublisherConfigChecker;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.wan.WanPublisherState;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class WanBatchPublisherConfigTest {

    private WanBatchPublisherConfig config = new WanBatchPublisherConfig();
    private static final WanBatchPublisherConfigChecker WAN_PUBLISHER_CONFIG_CHECKER
            = new WanBatchPublisherConfigChecker();

    @Test
    public void testSerialization() {
        Map<String, Comparable> properties = new HashMap<>();
        properties.put("key1", "value1");
        properties.put("key2", "value2");

        WanBatchPublisherConfig config = new WanBatchPublisherConfig()
                .setClusterName("myClusterName")
                .setPublisherId("myPublisherId")
                .setSnapshotEnabled(true)
                .setInitialPublisherState(WanPublisherState.STOPPED)
                .setQueueCapacity(23)
                .setBatchSize(500)
                .setBatchMaxDelayMillis(1000)
                .setResponseTimeoutMillis(60000)
                .setQueueFullBehavior(WanQueueFullBehavior.THROW_EXCEPTION)
                .setAcknowledgeType(WanAcknowledgeType.ACK_ON_OPERATION_COMPLETE)
                .setDiscoveryPeriodSeconds(20)
                .setMaxTargetEndpoints(100)
                .setMaxConcurrentInvocations(500)
                .setUseEndpointPrivateAddress(true)
                .setIdleMinParkNs(100)
                .setIdleMaxParkNs(1000)
                .setTargetEndpoints("a,b,c,d")
                .setDiscoveryConfig(new DiscoveryConfig())
                .setSyncConfig(new WanSyncConfig())
                .setAwsConfig(new AwsConfig().setEnabled(true).setProperty("connection-timeout-seconds", "20"))
                .setGcpConfig(new GcpConfig().setEnabled(true).setProperty("gcp", "gcp-val"))
                .setAzureConfig(new AzureConfig().setEnabled(true).setProperty("azure", "azure-val"))
                .setKubernetesConfig(new KubernetesConfig().setEnabled(true).setProperty("kubernetes", "kubernetes-val"))
                .setEurekaConfig(new EurekaConfig().setEnabled(true).setProperty("eureka", "eureka-val"))
                .setEndpoint("WAN")
                .setProperties(properties)
                .setImplementation(new DummyWanPublisher());

        SerializationService serializationService = new DefaultSerializationServiceBuilder().build();
        Data serialized = serializationService.toData(config);
        WanBatchPublisherConfig deserialized = serializationService.toObject(serialized);

        assertWanPublisherConfig(config, deserialized);
    }

    static void assertWanPublisherConfig(WanBatchPublisherConfig expected,
                                         WanBatchPublisherConfig actual) {
        WAN_PUBLISHER_CONFIG_CHECKER.check(expected, actual);
        assertEquals(expected.toString(), actual.toString());
    }
}
