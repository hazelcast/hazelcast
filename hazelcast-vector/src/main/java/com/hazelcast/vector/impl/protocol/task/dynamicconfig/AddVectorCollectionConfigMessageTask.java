/*
 * Copyright (c) 2008-2026, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.vector.impl.protocol.task.dynamicconfig;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.DynamicConfigAddVectorCollectionConfigCodec;
import com.hazelcast.client.impl.protocol.task.dynamicconfig.AbstractAddConfigMessageTask;
import com.hazelcast.config.MergePolicyConfig;
import com.hazelcast.config.vector.VectorCollectionConfig;
import com.hazelcast.config.vector.VectorIndexConfig;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.dynamicconfig.DynamicConfigurationAwareConfig;
import com.hazelcast.internal.nio.Connection;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.security.SecurityInterceptorConstants;
import com.hazelcast.security.permission.ActionConstants;
import com.hazelcast.security.permission.UserCodeNamespacePermission;

import java.security.Permission;

public class AddVectorCollectionConfigMessageTask
        extends AbstractAddConfigMessageTask<DynamicConfigAddVectorCollectionConfigCodec.RequestParameters> {

    public AddVectorCollectionConfigMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected DynamicConfigAddVectorCollectionConfigCodec.RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        return DynamicConfigAddVectorCollectionConfigCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return DynamicConfigAddVectorCollectionConfigCodec.encodeResponse();
    }

    @Override
    protected IdentifiedDataSerializable getConfig() {
        VectorCollectionConfig config = new VectorCollectionConfig();
        config.setName(parameters.name);

        // old clients that do not send backup counts will have backups disabled
        config.setBackupCount(parameters.backupCount);
        config.setAsyncBackupCount(parameters.asyncBackupCount);
        config.setSplitBrainProtectionName(parameters.splitBrainProtectionName);
        config.setUserCodeNamespace(parameters.userCodeNamespace);
        if (parameters.isMergeBatchSizeExists && parameters.isMergePolicyExists) {
            config.setMergePolicyConfig(new MergePolicyConfig(parameters.mergePolicy, parameters.mergeBatchSize));
        }

        for (VectorIndexConfig indexConfig : parameters.indexConfigs) {
            config.addVectorIndexConfig(indexConfig);
        }
        return config;
    }

    @Override
    public String getMethodName() {
        return SecurityInterceptorConstants.ADD_VECTOR_COLLECTION_CONFIG;
    }

    @Override
    public Permission getUserCodeNamespacePermission() {
        return parameters.userCodeNamespace != null
                ? new UserCodeNamespacePermission(parameters.userCodeNamespace, ActionConstants.ACTION_USE) : null;
    }

    @Override
    protected boolean checkStaticConfigDoesNotExist(IdentifiedDataSerializable config) {
        DynamicConfigurationAwareConfig nodeConfig = (DynamicConfigurationAwareConfig) nodeEngine.getConfig();
        VectorCollectionConfig vectorCollectionConfig = (VectorCollectionConfig) config;
        return DynamicConfigurationAwareConfig.checkStaticConfigDoesNotExist(
                nodeConfig.getStaticConfig().getVectorCollectionConfigs(),
                vectorCollectionConfig.getName(),
                vectorCollectionConfig);
    }
}
