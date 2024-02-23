/*
 * Copyright (c) 2008-2024, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.MCAddWanBatchPublisherConfigCodec;
import com.hazelcast.client.impl.protocol.task.management.AddWanBatchPublisherConfigMessageTask;
import com.hazelcast.config.ConsistencyCheckStrategy;
import com.hazelcast.config.WanAcknowledgeType;
import com.hazelcast.config.WanBatchPublisherConfig;
import com.hazelcast.config.WanQueueFullBehavior;
import com.hazelcast.config.WanReplicationConfig;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.verify;

public class AddWanBatchPublisherConfigMessageTaskTest extends ConfigMessageTaskTest {

    @Test
    public void testWanConfig() {
        ArgumentCaptor<WanReplicationConfig> wanCaptor = ArgumentCaptor.forClass(WanReplicationConfig.class);
        ClientMessage message = MCAddWanBatchPublisherConfigCodec.encodeRequest(
                "wan-config-name",
                "targetCluster",
                "publisherId",
                "192.168.19.20:5701",
                500,
                10,
                1000,
                1000,
                WanAcknowledgeType.ACK_ON_OPERATION_COMPLETE.getId(),
                WanQueueFullBehavior.THROW_EXCEPTION.getId(),
                ConsistencyCheckStrategy.MERKLE_TREES.getId()
        );
        AddWanBatchPublisherConfigMessageTask task = new AddWanBatchPublisherConfigMessageTask(message, mockNode, mockConnection);
        task.run();

        verify(mockNodeEngineImpl.getWanReplicationService()).addWanReplicationConfig(wanCaptor.capture());
        WanReplicationConfig wanReplicationConfig = wanCaptor.getValue();

        assertEquals("wan-config-name", wanReplicationConfig.getName());
        WanBatchPublisherConfig publisherConfig = wanReplicationConfig.getBatchPublisherConfigs().get(0);
        assertEquals("publisherId", publisherConfig.getPublisherId());
        assertEquals("targetCluster", publisherConfig.getClusterName());
        assertEquals("192.168.19.20:5701", publisherConfig.getTargetEndpoints());
        assertEquals(500, publisherConfig.getQueueCapacity());
        assertEquals(10, publisherConfig.getBatchSize());
        assertEquals(1000, publisherConfig.getBatchMaxDelayMillis());
        assertEquals(1000, publisherConfig.getResponseTimeoutMillis());
        assertEquals(WanAcknowledgeType.ACK_ON_OPERATION_COMPLETE, publisherConfig.getAcknowledgeType());
        assertEquals(WanQueueFullBehavior.THROW_EXCEPTION, publisherConfig.getQueueFullBehavior());
        assertEquals(ConsistencyCheckStrategy.MERKLE_TREES, publisherConfig.getSyncConfig().getConsistencyCheckStrategy());
    }
}
