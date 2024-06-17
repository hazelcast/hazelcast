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

package com.hazelcast.client;

import com.hazelcast.client.config.SubsetRoutingConfig;
import com.hazelcast.client.impl.ClientExtension;
import com.hazelcast.client.impl.clientside.DefaultClientExtension;
import com.hazelcast.client.impl.spi.ClientProxyFactory;
import com.hazelcast.client.impl.spi.impl.ClientClusterServiceImpl;
import com.hazelcast.config.InvalidConfigurationException;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.LoggingService;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.assertj.core.api.Assertions;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClientExtensionTest extends HazelcastTestSupport {

    @Test
    public void test_createServiceProxyFactory() {
        ClientExtension clientExtension = new DefaultClientExtension();
        assertInstanceOf(ClientProxyFactory.class, clientExtension.createServiceProxyFactory(MapService.class));
    }

    @Test
    public void test_createServiceProxyFactory_whenUnknownServicePassed() {
        ClientExtension clientExtension = new DefaultClientExtension();
        Assertions.assertThatThrownBy(() -> clientExtension.createServiceProxyFactory(TestService.class))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Proxy factory cannot be created. Unknown service");
    }

    @Test
    public void test_createClientClusterServiceWithSubsetRouting_throwsInvalidConfigurationException() {
        String expectedMsg = "Subset routing is an enterprise feature since 5.5. "
                + "You must use Hazelcast enterprise to enable this feature.";
        ClientExtension clientExtension = new DefaultClientExtension();
        SubsetRoutingConfig subsetRoutingConfig = new SubsetRoutingConfig().setEnabled(true);

        Assertions.assertThatThrownBy(() -> clientExtension.createClientClusterService(null, subsetRoutingConfig))
                .isInstanceOf(InvalidConfigurationException.class)
                .hasMessageContaining(expectedMsg);
    }

    @Test
    public void test_createClientClusterService() {
        ClientExtension clientExtension = new DefaultClientExtension();
        LoggingService loggingService = mock(LoggingService.class);
        when(loggingService.getLogger(any(Class.class))).thenReturn(mock(ILogger.class));
        assertInstanceOf(ClientClusterServiceImpl.class,
                clientExtension.createClientClusterService(loggingService, new SubsetRoutingConfig()));
    }

    private static class TestService {

    }
}
