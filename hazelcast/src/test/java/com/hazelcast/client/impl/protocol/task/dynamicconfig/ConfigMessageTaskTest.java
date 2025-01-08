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

import com.hazelcast.client.impl.ClientEndpoint;
import com.hazelcast.client.impl.ClientEndpointManager;
import com.hazelcast.client.impl.ClientEngine;
import com.hazelcast.client.impl.protocol.ClientExceptionFactory;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.task.AbstractMessageTask;
import com.hazelcast.config.Config;
import com.hazelcast.instance.impl.NodeExtension;
import com.hazelcast.internal.dynamicconfig.ClusterWideConfigurationService;
import com.hazelcast.internal.dynamicconfig.ConfigurationService;
import com.hazelcast.internal.dynamicconfig.DynamicConfigurationAwareConfig;
import com.hazelcast.internal.nio.Connection;
import com.hazelcast.internal.nio.ConnectionType;
import com.hazelcast.internal.server.ServerConnection;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.impl.InternalCompletableFuture;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.properties.HazelcastProperties;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.cluster.Address.createUnresolvedAddress;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public abstract class ConfigMessageTaskTest<V extends AbstractMessageTask<?>> {
    protected Connection mockConnection;
    protected NodeEngine mockNodeEngine;
    protected ClientEngine mockClientEngine;
    protected Config config;
    protected ILogger logger;
    protected NodeExtension mockNodeExtension;

    @Before
    public void setup() {
        // setup mocks
        mockConnection = mock(ServerConnection.class);
        mockClientEngine = mock(ClientEngine.class, RETURNS_DEEP_STUBS);
        config = new Config();
        logger = Logger.getLogger(getClass());
        ClientEndpointManager mockClientEndpointManager = mock(ClientEndpointManager.class);
        ClientEndpoint mockClientEndpoint = mock(ClientEndpoint.class);
        mockNodeEngine = mock(NodeEngine.class, RETURNS_DEEP_STUBS);
        mockNodeExtension = mock(NodeExtension.class);
        ClusterWideConfigurationService mockConfigurationService = mock(ClusterWideConfigurationService.class);
        InternalCompletableFuture<Object> mockFuture = mock(InternalCompletableFuture.class);

        when(mockClientEngine.getEndpointManager()).thenReturn(mockClientEndpointManager);
        when(mockClientEngine.getExceptionFactory()).thenReturn(new ClientExceptionFactory(false,
                config.getClassLoader()));
        when(mockClientEngine.getManagementTasksChecker().isTrusted(any())).thenReturn(true);

        when(mockClientEndpoint.getClientType()).thenReturn(ConnectionType.JAVA_CLIENT);
        when(mockClientEndpoint.isAuthenticated()).thenReturn(true);
        when(mockClientEndpointManager.getEndpoint(mockConnection)).thenReturn(mockClientEndpoint);

        when(mockNodeEngine.getConfig())
                .thenReturn(new DynamicConfigurationAwareConfig(config, new HazelcastProperties(config)));
        when(mockNodeEngine.getService(ConfigurationService.SERVICE_NAME)).thenReturn(mockConfigurationService);

        when(mockConfigurationService.broadcastConfigAsync(any(IdentifiedDataSerializable.class)))
                .thenReturn(mockFuture);
        when(mockNodeExtension.isStartCompleted()).thenReturn(true);
        when(mockConnection.getRemoteAddress()).thenReturn(createUnresolvedAddress("127.0.0.1", 5701));
    }

    protected abstract V createMessageTask(ClientMessage clientMessage);
}
