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
import com.hazelcast.client.impl.protocol.codec.MCSetDiagnosticsConfigCodec;
import com.hazelcast.client.impl.protocol.task.dynamicconfig.AbstractAddConfigMessageTask;
import com.hazelcast.config.Config;
import com.hazelcast.internal.diagnostics.DiagnosticsConfig;
import com.hazelcast.internal.diagnostics.DiagnosticsOutputType;
import com.hazelcast.instance.BuildInfo;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.instance.impl.NodeExtension;
import com.hazelcast.internal.cluster.impl.ClusterServiceImpl;
import com.hazelcast.internal.nio.Connection;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.security.permission.ManagementPermission;
import com.hazelcast.spi.impl.NodeEngine;

import java.security.Permission;

public class SetDiagnosticsConfigMessageTask
        extends AbstractAddConfigMessageTask<MCSetDiagnosticsConfigCodec.RequestParameters> {

    private static final Permission REQUIRED_PERMISSION = new ManagementPermission("cluster.setDiagnosticsConfig");

    public SetDiagnosticsConfigMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    public SetDiagnosticsConfigMessageTask(ClientMessage clientMessage, ILogger logger, NodeEngine nodeEngine,
                                           InternalSerializationService serializationService, ClientEngine clientEngine,
                                           Connection connection, NodeExtension nodeExtension, BuildInfo buildInfo,
                                           Config config, ClusterServiceImpl clusterService) {
        super(clientMessage, logger, nodeEngine, serializationService, clientEngine, connection, nodeExtension, buildInfo,
                config, clusterService);
    }

    @Override
    protected MCSetDiagnosticsConfigCodec.RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        return MCSetDiagnosticsConfigCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return MCSetDiagnosticsConfigCodec.encodeResponse();
    }

    @Override
    public Permission getRequiredPermission() {
        return REQUIRED_PERMISSION;
    }

    @Override
    protected IdentifiedDataSerializable getConfig() {
        DiagnosticsConfig diagnosticsConfig = new DiagnosticsConfig()
                .setEnabled(parameters.enabled)
                .setIncludeEpochTime(parameters.includeEpochTime)
                .setOutputType(DiagnosticsOutputType.valueOf(parameters.outputType))
                .setMaxRolledFileCount(parameters.maxRolledFileCount)
                .setMaxRolledFileSizeInMB(parameters.maxRolledFileSizeInMB)
                .setLogDirectory(parameters.logDirectory)
                .setFileNamePrefix(parameters.fileNamePrefix)
                .setAutoOffDurationInMinutes(parameters.autoOffDurationInMinutes);

        if (parameters.properties != null && !parameters.properties.isEmpty()) {
            diagnosticsConfig.getPluginProperties().putAll(parameters.properties);
        }

        return diagnosticsConfig;
    }


    @Override
    public String getMethodName() {
        return "setDiagnosticsConfig";
    }

    @Override
    protected boolean checkStaticConfigDoesNotExist(IdentifiedDataSerializable config) {
        return true;
    }

    @Override
    public boolean isManagementTask() {
        return true;
    }
}
