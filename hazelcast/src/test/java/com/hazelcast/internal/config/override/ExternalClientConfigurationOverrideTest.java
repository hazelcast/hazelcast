/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.github.stefanbirkner.systemlambda.SystemLambda.withEnvironmentVariable;
import static com.hazelcast.client.config.ClientConnectionStrategyConfig.ReconnectMode.ASYNC;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class ExternalClientConfigurationOverrideTest extends HazelcastTestSupport {

    @Test
    public void shouldExtractConfigFromEnv() throws Exception {
        ClientConfig config = new ClientConfig();
        withEnvironmentVariable("HZCLIENT_INSTANCENAME", "test")
          .and("HZCLIENT_NETWORK_AUTODETECTION_ENABLED", "false")
          .execute(() -> new ExternalConfigurationOverride().overwriteClientConfig(config));

        assertEquals("test", config.getInstanceName());
        assertFalse(config.getNetworkConfig().isAutoDetectionEnabled());
    }

    @Test
    public void shouldHandleConnectionStrategy() throws Exception {
        ClientConfig config = new ClientConfig();
        config.getConnectionStrategyConfig()
          .setReconnectMode(ASYNC)
          .setAsyncStart(true);

        withEnvironmentVariable("HZCLIENT_CONNECTIONSTRATEGY_ASYNCSTART", "false")
          .execute(() -> new ExternalConfigurationOverride().overwriteClientConfig(config));

        assertFalse(config.getConnectionStrategyConfig().isAsyncStart());
        assertEquals(ASYNC, config.getConnectionStrategyConfig().getReconnectMode());
    }
    @Test
    public void shouldHandleCustomPropertiesConfig() throws Exception {
        ClientConfig config = new ClientConfig();

        withEnvironmentVariable("HZCLIENT_PROPERTIES_foo", "bar")
          .execute(() -> new ExternalConfigurationOverride().overwriteClientConfig(config));

        assertEquals("bar", config.getProperty("foo"));
    }
}
