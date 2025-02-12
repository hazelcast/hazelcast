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

package com.hazelcast.client.impl.protocol.task.dynamicconfig;

import com.hazelcast.client.impl.ClientEngine;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.DynamicConfigSetDiagnosticsConfigCodec;
import com.hazelcast.config.Config;
import com.hazelcast.config.DiagnosticsConfig;
import com.hazelcast.config.DiagnosticsOutputType;
import com.hazelcast.instance.BuildInfo;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.instance.impl.NodeExtension;
import com.hazelcast.internal.cluster.impl.ClusterServiceImpl;
import com.hazelcast.internal.nio.Connection;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.security.SecurityInterceptorConstants;
import com.hazelcast.spi.impl.NodeEngine;

public class SetDiagnosticsConfigMessageTask
        extends AbstractAddConfigMessageTask<DynamicConfigSetDiagnosticsConfigCodec.RequestParameters> {

    public SetDiagnosticsConfigMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    protected SetDiagnosticsConfigMessageTask(ClientMessage clientMessage, ILogger logger, NodeEngine nodeEngine,
                                              InternalSerializationService serializationService, ClientEngine clientEngine,
                                              Connection connection, NodeExtension nodeExtension, BuildInfo buildInfo,
                                              Config config, ClusterServiceImpl clusterService) {
        super(clientMessage, logger, nodeEngine, serializationService, clientEngine, connection, nodeExtension, buildInfo,
                config, clusterService);
    }

    @Override
    protected DynamicConfigSetDiagnosticsConfigCodec.RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        return DynamicConfigSetDiagnosticsConfigCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return DynamicConfigSetDiagnosticsConfigCodec.encodeResponse();
    }

    @Override
    @SuppressWarnings({"checkstyle:npathcomplexity", "checkstyle:cyclomaticcomplexity", "checkstyle:MethodLength"})
    protected IdentifiedDataSerializable getConfig() {
        return new DiagnosticsConfig()
                .setEnabled(parameters.enabled)
                .setOutputType(DiagnosticsOutputType.valueOf(parameters.outputType))
                .setIncludeEpochTime(parameters.includeEpochTime)
                .setMaxRolledFileSizeInMB(parameters.maxRolledFileSizeInMB)
                .setMaxRolledFileCount(parameters.maxRolledFileCount)
                .setLogDirectory(parameters.logDirectory)
                .setFileNamePrefix(parameters.fileNamePrefix);
    }


    @Override
    public String getMethodName() {
        return SecurityInterceptorConstants.SET_DIAGNOSTICS_CONFIG;
    }

    @Override
    protected boolean checkStaticConfigDoesNotExist(IdentifiedDataSerializable config) {
        return true;
    }
}
