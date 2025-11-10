/*
 * Copyright (c) 2008-2025, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.impl.protocol.codec.custom;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.Generated;
import com.hazelcast.client.impl.protocol.codec.builtin.*;

import static com.hazelcast.client.impl.protocol.codec.builtin.CodecUtil.fastForwardToEndFrame;
import static com.hazelcast.client.impl.protocol.ClientMessage.*;
import static com.hazelcast.client.impl.protocol.codec.builtin.FixedSizeTypesCodec.*;

@SuppressWarnings("unused")
@Generated("5693894eef8e0beb7db9c143865a2082")
public final class WanBatchPublisherConfigHolderCodec {
    private static final int SNAPSHOT_ENABLED_FIELD_OFFSET = 0;
    private static final int INITIAL_PUBLISHER_STATE_FIELD_OFFSET = SNAPSHOT_ENABLED_FIELD_OFFSET + BOOLEAN_SIZE_IN_BYTES;
    private static final int QUEUE_CAPACITY_FIELD_OFFSET = INITIAL_PUBLISHER_STATE_FIELD_OFFSET + BYTE_SIZE_IN_BYTES;
    private static final int BATCH_SIZE_FIELD_OFFSET = QUEUE_CAPACITY_FIELD_OFFSET + INT_SIZE_IN_BYTES;
    private static final int BATCH_MAX_DELAY_MILLIS_FIELD_OFFSET = BATCH_SIZE_FIELD_OFFSET + INT_SIZE_IN_BYTES;
    private static final int RESPONSE_TIMEOUT_MILLIS_FIELD_OFFSET = BATCH_MAX_DELAY_MILLIS_FIELD_OFFSET + INT_SIZE_IN_BYTES;
    private static final int QUEUE_FULL_BEHAVIOR_FIELD_OFFSET = RESPONSE_TIMEOUT_MILLIS_FIELD_OFFSET + INT_SIZE_IN_BYTES;
    private static final int ACKNOWLEDGE_TYPE_FIELD_OFFSET = QUEUE_FULL_BEHAVIOR_FIELD_OFFSET + INT_SIZE_IN_BYTES;
    private static final int DISCOVERY_PERIOD_SECONDS_FIELD_OFFSET = ACKNOWLEDGE_TYPE_FIELD_OFFSET + INT_SIZE_IN_BYTES;
    private static final int MAX_TARGET_ENDPOINTS_FIELD_OFFSET = DISCOVERY_PERIOD_SECONDS_FIELD_OFFSET + INT_SIZE_IN_BYTES;
    private static final int MAX_CONCURRENT_INVOCATIONS_FIELD_OFFSET = MAX_TARGET_ENDPOINTS_FIELD_OFFSET + INT_SIZE_IN_BYTES;
    private static final int USE_ENDPOINT_PRIVATE_ADDRESS_FIELD_OFFSET = MAX_CONCURRENT_INVOCATIONS_FIELD_OFFSET + INT_SIZE_IN_BYTES;
    private static final int IDLE_MIN_PARK_NS_FIELD_OFFSET = USE_ENDPOINT_PRIVATE_ADDRESS_FIELD_OFFSET + BOOLEAN_SIZE_IN_BYTES;
    private static final int IDLE_MAX_PARK_NS_FIELD_OFFSET = IDLE_MIN_PARK_NS_FIELD_OFFSET + LONG_SIZE_IN_BYTES;
    private static final int INITIAL_FRAME_SIZE = IDLE_MAX_PARK_NS_FIELD_OFFSET + LONG_SIZE_IN_BYTES;

    private WanBatchPublisherConfigHolderCodec() {
    }

    public static void encode(ClientMessage clientMessage, com.hazelcast.client.impl.protocol.codec.holder.WanBatchPublisherConfigHolder wanBatchPublisherConfigHolder) {
        clientMessage.add(BEGIN_FRAME.copy());

        ClientMessage.Frame initialFrame = new ClientMessage.Frame(new byte[INITIAL_FRAME_SIZE]);
        encodeBoolean(initialFrame.content, SNAPSHOT_ENABLED_FIELD_OFFSET, wanBatchPublisherConfigHolder.isSnapshotEnabled());
        encodeByte(initialFrame.content, INITIAL_PUBLISHER_STATE_FIELD_OFFSET, wanBatchPublisherConfigHolder.getInitialPublisherState());
        encodeInt(initialFrame.content, QUEUE_CAPACITY_FIELD_OFFSET, wanBatchPublisherConfigHolder.getQueueCapacity());
        encodeInt(initialFrame.content, BATCH_SIZE_FIELD_OFFSET, wanBatchPublisherConfigHolder.getBatchSize());
        encodeInt(initialFrame.content, BATCH_MAX_DELAY_MILLIS_FIELD_OFFSET, wanBatchPublisherConfigHolder.getBatchMaxDelayMillis());
        encodeInt(initialFrame.content, RESPONSE_TIMEOUT_MILLIS_FIELD_OFFSET, wanBatchPublisherConfigHolder.getResponseTimeoutMillis());
        encodeInt(initialFrame.content, QUEUE_FULL_BEHAVIOR_FIELD_OFFSET, wanBatchPublisherConfigHolder.getQueueFullBehavior());
        encodeInt(initialFrame.content, ACKNOWLEDGE_TYPE_FIELD_OFFSET, wanBatchPublisherConfigHolder.getAcknowledgeType());
        encodeInt(initialFrame.content, DISCOVERY_PERIOD_SECONDS_FIELD_OFFSET, wanBatchPublisherConfigHolder.getDiscoveryPeriodSeconds());
        encodeInt(initialFrame.content, MAX_TARGET_ENDPOINTS_FIELD_OFFSET, wanBatchPublisherConfigHolder.getMaxTargetEndpoints());
        encodeInt(initialFrame.content, MAX_CONCURRENT_INVOCATIONS_FIELD_OFFSET, wanBatchPublisherConfigHolder.getMaxConcurrentInvocations());
        encodeBoolean(initialFrame.content, USE_ENDPOINT_PRIVATE_ADDRESS_FIELD_OFFSET, wanBatchPublisherConfigHolder.isUseEndpointPrivateAddress());
        encodeLong(initialFrame.content, IDLE_MIN_PARK_NS_FIELD_OFFSET, wanBatchPublisherConfigHolder.getIdleMinParkNs());
        encodeLong(initialFrame.content, IDLE_MAX_PARK_NS_FIELD_OFFSET, wanBatchPublisherConfigHolder.getIdleMaxParkNs());
        clientMessage.add(initialFrame);

        CodecUtil.encodeNullable(clientMessage, wanBatchPublisherConfigHolder.getPublisherId(), StringCodec::encode);
        CodecUtil.encodeNullable(clientMessage, wanBatchPublisherConfigHolder.getClassName(), StringCodec::encode);
        DataCodec.encodeNullable(clientMessage, wanBatchPublisherConfigHolder.getImplementation());
        MapCodec.encode(clientMessage, wanBatchPublisherConfigHolder.getProperties(), StringCodec::encode, DataCodec::encode);
        CodecUtil.encodeNullable(clientMessage, wanBatchPublisherConfigHolder.getClusterName(), StringCodec::encode);
        StringCodec.encode(clientMessage, wanBatchPublisherConfigHolder.getTargetEndpoints());
        AwsConfigCodec.encode(clientMessage, wanBatchPublisherConfigHolder.getAwsConfig());
        GcpConfigCodec.encode(clientMessage, wanBatchPublisherConfigHolder.getGcpConfig());
        AzureConfigCodec.encode(clientMessage, wanBatchPublisherConfigHolder.getAzureConfig());
        KubernetesConfigCodec.encode(clientMessage, wanBatchPublisherConfigHolder.getKubernetesConfig());
        EurekaConfigCodec.encode(clientMessage, wanBatchPublisherConfigHolder.getEurekaConfig());
        DiscoveryConfigCodec.encode(clientMessage, wanBatchPublisherConfigHolder.getDiscoveryConfig());
        WanSyncConfigCodec.encode(clientMessage, wanBatchPublisherConfigHolder.getSyncConfig());
        CodecUtil.encodeNullable(clientMessage, wanBatchPublisherConfigHolder.getEndpoint(), StringCodec::encode);

        clientMessage.add(END_FRAME.copy());
    }

    public static com.hazelcast.client.impl.protocol.codec.holder.WanBatchPublisherConfigHolder decode(ClientMessage.ForwardFrameIterator iterator) {
        // begin frame
        iterator.next();

        ClientMessage.Frame initialFrame = iterator.next();
        boolean snapshotEnabled = decodeBoolean(initialFrame.content, SNAPSHOT_ENABLED_FIELD_OFFSET);
        byte initialPublisherState = decodeByte(initialFrame.content, INITIAL_PUBLISHER_STATE_FIELD_OFFSET);
        int queueCapacity = decodeInt(initialFrame.content, QUEUE_CAPACITY_FIELD_OFFSET);
        int batchSize = decodeInt(initialFrame.content, BATCH_SIZE_FIELD_OFFSET);
        int batchMaxDelayMillis = decodeInt(initialFrame.content, BATCH_MAX_DELAY_MILLIS_FIELD_OFFSET);
        int responseTimeoutMillis = decodeInt(initialFrame.content, RESPONSE_TIMEOUT_MILLIS_FIELD_OFFSET);
        int queueFullBehavior = decodeInt(initialFrame.content, QUEUE_FULL_BEHAVIOR_FIELD_OFFSET);
        int acknowledgeType = decodeInt(initialFrame.content, ACKNOWLEDGE_TYPE_FIELD_OFFSET);
        int discoveryPeriodSeconds = decodeInt(initialFrame.content, DISCOVERY_PERIOD_SECONDS_FIELD_OFFSET);
        int maxTargetEndpoints = decodeInt(initialFrame.content, MAX_TARGET_ENDPOINTS_FIELD_OFFSET);
        int maxConcurrentInvocations = decodeInt(initialFrame.content, MAX_CONCURRENT_INVOCATIONS_FIELD_OFFSET);
        boolean useEndpointPrivateAddress = decodeBoolean(initialFrame.content, USE_ENDPOINT_PRIVATE_ADDRESS_FIELD_OFFSET);
        long idleMinParkNs = decodeLong(initialFrame.content, IDLE_MIN_PARK_NS_FIELD_OFFSET);
        long idleMaxParkNs = decodeLong(initialFrame.content, IDLE_MAX_PARK_NS_FIELD_OFFSET);

        java.lang.String publisherId = CodecUtil.decodeNullable(iterator, StringCodec::decode);
        java.lang.String className = CodecUtil.decodeNullable(iterator, StringCodec::decode);
        com.hazelcast.internal.serialization.Data implementation = DataCodec.decodeNullable(iterator);
        java.util.Map<java.lang.String, com.hazelcast.internal.serialization.Data> properties = MapCodec.decode(iterator, StringCodec::decode, DataCodec::decode);
        java.lang.String clusterName = CodecUtil.decodeNullable(iterator, StringCodec::decode);
        java.lang.String targetEndpoints = StringCodec.decode(iterator);
        com.hazelcast.config.AwsConfig awsConfig = AwsConfigCodec.decode(iterator);
        com.hazelcast.config.GcpConfig gcpConfig = GcpConfigCodec.decode(iterator);
        com.hazelcast.config.AzureConfig azureConfig = AzureConfigCodec.decode(iterator);
        com.hazelcast.config.KubernetesConfig kubernetesConfig = KubernetesConfigCodec.decode(iterator);
        com.hazelcast.config.EurekaConfig eurekaConfig = EurekaConfigCodec.decode(iterator);
        com.hazelcast.client.impl.protocol.codec.holder.DiscoveryConfigHolder discoveryConfig = DiscoveryConfigCodec.decode(iterator);
        com.hazelcast.client.impl.protocol.codec.holder.WanSyncConfigHolder syncConfig = WanSyncConfigCodec.decode(iterator);
        java.lang.String endpoint = CodecUtil.decodeNullable(iterator, StringCodec::decode);

        fastForwardToEndFrame(iterator);

        return new com.hazelcast.client.impl.protocol.codec.holder.WanBatchPublisherConfigHolder(publisherId, className, implementation, properties, clusterName, snapshotEnabled, initialPublisherState, queueCapacity, batchSize, batchMaxDelayMillis, responseTimeoutMillis, queueFullBehavior, acknowledgeType, discoveryPeriodSeconds, maxTargetEndpoints, maxConcurrentInvocations, useEndpointPrivateAddress, idleMinParkNs, idleMaxParkNs, targetEndpoints, awsConfig, gcpConfig, azureConfig, kubernetesConfig, eurekaConfig, discoveryConfig, syncConfig, endpoint);
    }
}
