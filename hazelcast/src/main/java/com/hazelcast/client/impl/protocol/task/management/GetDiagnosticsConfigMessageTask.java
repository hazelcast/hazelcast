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

package com.hazelcast.client.impl.protocol.task.management;

import com.hazelcast.client.impl.ClientEngine;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.MCGetDiagnosticsConfigCodec;
import com.hazelcast.client.impl.protocol.task.AbstractCallableMessageTask;
import com.hazelcast.config.Config;
import com.hazelcast.instance.BuildInfo;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.instance.impl.NodeExtension;
import com.hazelcast.internal.cluster.impl.ClusterServiceImpl;
import com.hazelcast.internal.diagnostics.Diagnostics;
import com.hazelcast.internal.diagnostics.DiagnosticsConfig;
import com.hazelcast.internal.nio.Connection;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.logging.ILogger;
import com.hazelcast.security.permission.ManagementPermission;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.NodeEngineImpl;

import java.security.Permission;

public class GetDiagnosticsConfigMessageTask
        extends AbstractCallableMessageTask<Void> {

    private static final Permission REQUIRED_PERMISSION = new ManagementPermission("cluster.getDiagnosticsConfig");

    class OverloadedDiagnosticsConfig {

        final DiagnosticsConfig diagnosticsConfig;
        final boolean canEnabledDynamically;

        OverloadedDiagnosticsConfig(DiagnosticsConfig diagnosticsConfig, boolean canEnabledDynamically) {
            this.diagnosticsConfig = diagnosticsConfig;
            this.canEnabledDynamically = canEnabledDynamically;
        }
    }

    public GetDiagnosticsConfigMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    public GetDiagnosticsConfigMessageTask(ClientMessage clientMessage, ILogger logger, NodeEngine nodeEngine,
                                           InternalSerializationService serializationService, ClientEngine clientEngine,
                                           Connection connection, NodeExtension nodeExtension, BuildInfo buildInfo,
                                           Config config, ClusterServiceImpl clusterService) {
        super(clientMessage, logger, nodeEngine, serializationService, clientEngine, connection, nodeExtension, buildInfo,
                config, clusterService);
    }

    @Override
    protected Void decodeClientMessage(ClientMessage clientMessage) {
        return null;
    }

    @Override
    protected Object call() {
        if (nodeEngine instanceof NodeEngineImpl nodeImpl) {
            Diagnostics diagnostics = nodeImpl.getDiagnostics();
            return new OverloadedDiagnosticsConfig(
                    diagnostics.getDiagnosticsConfig(),
                    !diagnostics.isConfiguredStatically()
            );
        }
        throw new IllegalStateException("NodeEngine is not an instance of NodeEngineImpl");
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        OverloadedDiagnosticsConfig config = (OverloadedDiagnosticsConfig) response;

        return MCGetDiagnosticsConfigCodec.encodeResponse(
                config.diagnosticsConfig.isEnabled(),
                config.diagnosticsConfig.getOutputType().name(),
                config.diagnosticsConfig.isIncludeEpochTime(),
                config.diagnosticsConfig.getMaxRolledFileSizeInMB(),
                config.diagnosticsConfig.getMaxRolledFileCount(),
                config.diagnosticsConfig.getLogDirectory(),
                config.diagnosticsConfig.getFileNamePrefix(),
                config.diagnosticsConfig.getPluginProperties(),
                config.diagnosticsConfig.getAutoOffDurationInMinutes(),
                config.canEnabledDynamically
        );
    }

    @Override
    public String getServiceName() {
        return "";
    }

    @Override
    public Permission getRequiredPermission() {
        return REQUIRED_PERMISSION;
    }

    @Override
    public String getDistributedObjectName() {
        return null;
    }

    @Override
    public String getMethodName() {
        return "getDiagnosticsConfig";
    }

    @Override
    public Object[] getParameters() {
        return new Object[0];
    }

    @Override
    public boolean isManagementTask() {
        return true;
    }
}
