/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.management.dto;

import com.hazelcast.config.AwsConfig;
import com.hazelcast.config.AzureConfig;
import com.hazelcast.config.ConfigCompatibilityChecker.WanBatchReplicationPublisherConfigChecker;
import com.hazelcast.config.DiscoveryConfig;
import com.hazelcast.config.EurekaConfig;
import com.hazelcast.config.GcpConfig;
import com.hazelcast.config.KubernetesConfig;
import com.hazelcast.config.WanQueueFullBehavior;
import com.hazelcast.config.WanAcknowledgeType;
import com.hazelcast.config.WanBatchReplicationPublisherConfig;
import com.hazelcast.config.WanPublisherState;
import com.hazelcast.config.WanSyncConfig;
import com.hazelcast.internal.json.JsonObject;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class WanBatchReplicationPublisherConfigDTOTest {

    private static final WanBatchReplicationPublisherConfigChecker WAN_PUBLISHER_CONFIG_CHECKER
            = new WanBatchReplicationPublisherConfigChecker();

    @Test
    public void testSerialization() {
        Map<String, Comparable> properties = new HashMap<>();
        properties.put("key1", "value1");
        properties.put("key2", "value2");

        WanBatchReplicationPublisherConfig expected = new WanBatchReplicationPublisherConfig()
                .setGroupName("myGroupName")
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
                .setWanSyncConfig(new WanSyncConfig())
                .setAwsConfig(new AwsConfig().setEnabled(true).setConnectionTimeoutSeconds(20))
                .setGcpConfig(new GcpConfig().setEnabled(true).setProperty("gcp", "gcp-val"))
                .setAzureConfig(new AzureConfig().setEnabled(true).setProperty("azure", "azure-val"))
                .setKubernetesConfig(new KubernetesConfig().setEnabled(true).setProperty("kubernetes", "kubernetes-val"))
                .setEurekaConfig(new EurekaConfig().setEnabled(true).setProperty("eureka", "eureka-val"))
                .setEndpoint("WAN")
                .setProperties(properties);

        WanBatchReplicationPublisherConfig actual = cloneThroughJson(expected);
        assertTrue("Expected: " + expected + ", got:" + actual,
                WAN_PUBLISHER_CONFIG_CHECKER.check(expected, actual));
    }

    @Test
    public void testDefault() {
        WanBatchReplicationPublisherConfig expected = new WanBatchReplicationPublisherConfig();

        WanBatchReplicationPublisherConfig actual = cloneThroughJson(expected);
        assertTrue("Expected: " + expected + ", got:" + actual,
                WAN_PUBLISHER_CONFIG_CHECKER.check(expected, actual));
    }

    private WanBatchReplicationPublisherConfig cloneThroughJson(WanBatchReplicationPublisherConfig expected) {
        WanBatchReplicationPublisherConfigDTO dto = new WanBatchReplicationPublisherConfigDTO(expected);

        JsonObject json = dto.toJson();
        WanBatchReplicationPublisherConfigDTO deserialized = new WanBatchReplicationPublisherConfigDTO(null);
        deserialized.fromJson(json);

        return deserialized.getConfig();
    }

}
