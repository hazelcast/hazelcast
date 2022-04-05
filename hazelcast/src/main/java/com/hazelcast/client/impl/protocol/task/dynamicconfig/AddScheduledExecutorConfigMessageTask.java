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

package com.hazelcast.client.impl.protocol.task.dynamicconfig;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.DynamicConfigAddScheduledExecutorConfigCodec;
import com.hazelcast.config.MergePolicyConfig;
import com.hazelcast.config.ScheduledExecutorConfig;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.dynamicconfig.DynamicConfigurationAwareConfig;
import com.hazelcast.internal.nio.Connection;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

public class AddScheduledExecutorConfigMessageTask
        extends AbstractAddConfigMessageTask<DynamicConfigAddScheduledExecutorConfigCodec.RequestParameters> {

    public AddScheduledExecutorConfigMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected DynamicConfigAddScheduledExecutorConfigCodec.RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        return DynamicConfigAddScheduledExecutorConfigCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return DynamicConfigAddScheduledExecutorConfigCodec.encodeResponse();
    }

    @Override
    protected IdentifiedDataSerializable getConfig() {
        // This is to handle 4.0 client versions. Those
        // versions don't aware of `statisticsEnabled` parameter.
        // The parameter was added at version 4.1 and its default value is  true.
        boolean statsEnabled = !parameters.isStatisticsEnabledExists
                || parameters.statisticsEnabled;

        ScheduledExecutorConfig config = new ScheduledExecutorConfig();
        config.setPoolSize(parameters.poolSize);
        config.setDurability(parameters.durability);
        config.setCapacity(parameters.capacity);
        config.setName(parameters.name);
        config.setStatisticsEnabled(statsEnabled);
        MergePolicyConfig mergePolicyConfig = mergePolicyConfig(parameters.mergePolicy, parameters.mergeBatchSize);
        config.setMergePolicyConfig(mergePolicyConfig);
        return config;
    }

    @Override
    public String getMethodName() {
        return "addScheduledExecutorConfig";
    }

    @Override
    protected boolean checkStaticConfigDoesNotExist(IdentifiedDataSerializable config) {
        DynamicConfigurationAwareConfig nodeConfig = (DynamicConfigurationAwareConfig) nodeEngine.getConfig();
        ScheduledExecutorConfig scheduledExecutorConfig = (ScheduledExecutorConfig) config;
        return nodeConfig.checkStaticConfigDoesNotExist(nodeConfig.getStaticConfig().getScheduledExecutorConfigs(),
                scheduledExecutorConfig.getName(), scheduledExecutorConfig);
    }
}
