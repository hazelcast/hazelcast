/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.client.impl.protocol.codec.DynamicConfigAddCacheConfigCodec;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.config.CacheSimpleConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.DataPersistenceConfig;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.instance.impl.NodeExtension;
import com.hazelcast.internal.dynamicconfig.ClusterWideConfigurationService;
import com.hazelcast.internal.dynamicconfig.ConfigurationService;
import com.hazelcast.internal.dynamicconfig.DynamicConfigurationAwareConfig;
import com.hazelcast.internal.nio.Connection;
import com.hazelcast.internal.nio.ConnectionType;
import com.hazelcast.internal.server.ServerConnection;
import com.hazelcast.logging.Logger;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.impl.InternalCompletableFuture;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.properties.HazelcastProperties;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.cluster.Address.createUnresolvedAddress;
import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class AddCacheConfigMessageTaskTest {
    private Node mockNode;
    private Connection mockConnection;

    @Before
    public void setup() {
        // setup mocks
        mockNode = mock(Node.class);
        mockConnection = mock(ServerConnection.class);
        ClientEngine mockClientEngine = mock(ClientEngine.class);
        ClientEndpointManager mockClientEndpointManager = mock(ClientEndpointManager.class);
        ClientEndpoint mockClientEndpoint = mock(ClientEndpoint.class);
        NodeEngineImpl mockNodeEngineImpl = mock(NodeEngineImpl.class);
        NodeExtension mockNodeExtension = mock(NodeExtension.class);
        ClusterWideConfigurationService mockConfigurationService = mock(ClusterWideConfigurationService.class);
        InternalCompletableFuture<Object> mockFuture = mock(InternalCompletableFuture.class);

        when(mockNode.getClientEngine()).thenReturn(mockClientEngine);
        when(mockNode.getConfig()).thenReturn(new Config());
        when(mockNode.getLogger(any(Class.class))).thenReturn(Logger.getLogger(AddMapConfigMessageTaskTest.class));
        when(mockNode.getNodeExtension()).thenReturn(mockNodeExtension);
        when(mockNode.getNodeEngine()).thenReturn(mockNodeEngineImpl);

        when(mockClientEngine.getEndpointManager()).thenReturn(mockClientEndpointManager);
        when(mockClientEngine.getExceptionFactory()).thenReturn(new ClientExceptionFactory(false,
                new Config().getClassLoader()));

        when(mockClientEndpoint.getClientType()).thenReturn(ConnectionType.JAVA_CLIENT);
        when(mockClientEndpoint.isAuthenticated()).thenReturn(true);
        when(mockClientEndpointManager.getEndpoint(mockConnection)).thenReturn(mockClientEndpoint);

        when(mockNodeEngineImpl.getConfig()).thenReturn(new DynamicConfigurationAwareConfig(
                new Config(),
                new HazelcastProperties(new Config()))
        );
        when(mockNodeEngineImpl.getService(ConfigurationService.SERVICE_NAME)).thenReturn(mockConfigurationService);

        when(mockConfigurationService.broadcastConfigAsync(any(IdentifiedDataSerializable.class)))
                .thenReturn(mockFuture);
        when(mockNodeExtension.isStartCompleted()).thenReturn(true);
        when(mockConnection.getRemoteAddress()).thenReturn(createUnresolvedAddress("127.0.0.1", 5701));
    }


    @Test
    public void doNotThrowException_whenNullValuesProvidedForNullableFields() throws Exception {
        CacheConfig<Object, Object> cacheConfig = new CacheConfig<>("my-cache");
        ClientMessage addMapConfigClientMessage = DynamicConfigAddCacheConfigCodec.encodeRequest(
                cacheConfig.getName(),
                null,
                null,
                cacheConfig.isStatisticsEnabled(),
                cacheConfig.isManagementEnabled(),
                cacheConfig.isReadThrough(),
                cacheConfig.isWriteThrough(),
                null,
                null,
                null,
                null,
                cacheConfig.getBackupCount(),
                cacheConfig.getAsyncBackupCount(),
                cacheConfig.getInMemoryFormat().name(),
                null,
                null,
                0,
                cacheConfig.isDisablePerEntryInvalidationEvents(),
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                cacheConfig.getDataPersistenceConfig()
        );
        AddCacheConfigMessageTask addCacheConfigMessageTask = new AddCacheConfigMessageTask(addMapConfigClientMessage, mockNode, mockConnection);
        addCacheConfigMessageTask.run();
        CacheConfig transmittedCacheConfig = new CacheConfig((CacheSimpleConfig) addCacheConfigMessageTask.getConfig());
        assertEquals(cacheConfig, transmittedCacheConfig);
    }

    @Test
    public void testDataPersistenceSubConfigTransmittedCorrectly() throws Exception {
        CacheConfig<Object, Object> cacheConfig = new CacheConfig<>("my-cache");
        DataPersistenceConfig dataPersistenceConfig = new DataPersistenceConfig();
        dataPersistenceConfig.setEnabled(true);
        dataPersistenceConfig.setFsync(true);
        cacheConfig.setDataPersistenceConfig(dataPersistenceConfig);
        ClientMessage addMapConfigClientMessage = DynamicConfigAddCacheConfigCodec.encodeRequest(
                cacheConfig.getName(),
                null,
                null,
                cacheConfig.isStatisticsEnabled(),
                cacheConfig.isManagementEnabled(),
                cacheConfig.isReadThrough(),
                cacheConfig.isWriteThrough(),
                null,
                null,
                null,
                null,
                cacheConfig.getBackupCount(),
                cacheConfig.getAsyncBackupCount(),
                cacheConfig.getInMemoryFormat().name(),
                null,
                null,
                0,
                cacheConfig.isDisablePerEntryInvalidationEvents(),
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                cacheConfig.getDataPersistenceConfig()
        );
        AddCacheConfigMessageTask addCacheConfigMessageTask = new AddCacheConfigMessageTask(addMapConfigClientMessage, mockNode, mockConnection);
        addCacheConfigMessageTask.run();
        CacheConfig transmittedCacheConfig = new CacheConfig((CacheSimpleConfig) addCacheConfigMessageTask.getConfig());
        assertEquals(cacheConfig, transmittedCacheConfig);
    }
}
