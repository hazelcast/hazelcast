/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.client.impl.protocol.task.AbstractMessageTask;
import com.hazelcast.config.ListenerConfig;
import com.hazelcast.config.MergePolicyConfig;
import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.instance.Node;
import com.hazelcast.internal.dynamicconfig.ClusterWideConfigurationService;
import com.hazelcast.nio.Connection;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.security.permission.ConfigPermission;

import java.security.Permission;
import java.util.ArrayList;
import java.util.List;

/**
 * Base implementation for dynamic add***Config methods.
 */
public abstract class AbstractAddConfigMessageTask<P> extends AbstractMessageTask<P>
        implements ExecutionCallback<Object> {

    private static final ConfigPermission CONFIG_PERMISSION = new ConfigPermission();

    public AbstractAddConfigMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }


    @Override
    public String getServiceName() {
        return ClusterWideConfigurationService.SERVICE_NAME;
    }

    @Override
    public String getDistributedObjectName() {
        return null;
    }

    @Override
    public Permission getRequiredPermission() {
        return CONFIG_PERMISSION;
    }

    @Override
    public Object[] getParameters() {
        // todo may have to specify for security
        return new Object[0];
    }

    @Override
    public final void processMessage() {
        IdentifiedDataSerializable config = getConfig();
        ClusterWideConfigurationService service = getService(ClusterWideConfigurationService.SERVICE_NAME);
        ICompletableFuture<Object> future = service.broadcastConfigAsync(config);
        future.andThen(this);
    }

    @Override
    public void onResponse(Object response) {
        sendResponse(response);
    }

    @Override
    public void onFailure(Throwable t) {
        handleProcessingFailure(t);
    }

    // returns a MergePolicyConfig based on given parameters if these exist, or the default MergePolicyConfig
    protected MergePolicyConfig mergePolicyConfig(boolean mergePolicyExist, String mergePolicy, int batchSize) {
        if (mergePolicyExist) {
            MergePolicyConfig config = new MergePolicyConfig(mergePolicy, batchSize);
            return config;
        }
        return new MergePolicyConfig();
    }

    protected List<? extends ListenerConfig> adaptListenerConfigs(List<ListenerConfigHolder> listenerConfigHolders) {
        if (listenerConfigHolders == null || listenerConfigHolders.isEmpty()) {
            return null;
        }

        List<ListenerConfig> itemListenerConfigs = new ArrayList<ListenerConfig>();
        for (ListenerConfigHolder listenerConfigHolder : listenerConfigHolders) {
            itemListenerConfigs.add(listenerConfigHolder.asListenerConfig(serializationService));
        }
        return itemListenerConfigs;
    }

    protected abstract IdentifiedDataSerializable getConfig();
}
