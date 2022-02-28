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

package com.hazelcast.internal.config.override;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.HashMap;
import java.util.Map;

import static com.hazelcast.client.config.ClientConnectionStrategyConfig.ReconnectMode.ASYNC;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ExternalClientConfigurationOverrideTest extends HazelcastTestSupport {

    @Test
    public void shouldExtractConfigFromEnv() throws Exception {
        ClientConfig config = new ClientConfig();

        Map<String, String> systemEnvs = new HashMap<>();
        systemEnvs.put("HZCLIENT_INSTANCENAME", "test");
        systemEnvs.put("HZCLIENT_NETWORK_AUTODETECTION_ENABLED", "false");

        new ExternalConfigurationOverride(systemEnvs, System::getProperties).overwriteClientConfig(config);
        assertEquals("test", config.getInstanceName());
        assertFalse(config.getNetworkConfig().isAutoDetectionEnabled());
    }

    @Test
    public void shouldHandleConnectionStrategy() throws Exception {
        ClientConfig config = new ClientConfig();
        config.getConnectionStrategyConfig()
                .setReconnectMode(ASYNC)
                .setAsyncStart(true);

        Map<String, String> systemEnvs = new HashMap<>();
        systemEnvs.put("HZCLIENT_CONNECTIONSTRATEGY_ASYNCSTART", "false");

        new ExternalConfigurationOverride(systemEnvs, System::getProperties).overwriteClientConfig(config);

        assertFalse(config.getConnectionStrategyConfig().isAsyncStart());
        assertEquals(ASYNC, config.getConnectionStrategyConfig().getReconnectMode());
    }
    @Test
    public void shouldHandleCustomPropertiesConfig() throws Exception {
        ClientConfig config = new ClientConfig();

        Map<String, String> systemEnvs = new HashMap<>();
        systemEnvs.put("HZCLIENT_PROPERTIES_foo", "bar");

        new ExternalConfigurationOverride(systemEnvs, System::getProperties).overwriteClientConfig(config);

        assertEquals("bar", config.getProperty("foo"));
    }
}
