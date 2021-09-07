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

import com.hazelcast.config.Config;
import com.hazelcast.config.InvalidConfigurationException;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.config.RestEndpointGroup.*;
import static com.hazelcast.internal.config.override.ExternalConfigTestUtils.entry;
import static com.hazelcast.internal.config.override.ExternalConfigTestUtils.runWithSystemProperties;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class ExternalMemberConfigurationOverrideSystemPropertiesTest extends HazelcastTestSupport {

    @Test
    public void shouldExtractConfigFromSysProperties() {
        runWithSystemProperties(() -> {
              Config config = new Config();
              new ExternalConfigurationOverride().overwriteMemberConfig(config);

              assertEquals("test", config.getClusterName());
              assertEquals(42, config.getExecutorConfig("custom").getPoolSize());
              assertFalse(config.getNetworkConfig().getJoin().isAutoDetectionEnabled());
          },
          entry("hz.cluster-name", "test"),
          entry("hz.network.join.autodetection.enabled", "false"),
          entry("hz.executor-service.custom.pool-size", "42"));
    }

    @Test
    public void shouldHandleRestApiConfigFromSysProperties() {
        runWithSystemProperties(() -> {
              Config config = new Config();
              new ExternalConfigurationOverride().overwriteMemberConfig(config);

              assertTrue(config.getNetworkConfig().getRestApiConfig().isEnabled());
              assertTrue(config.getNetworkConfig().getRestApiConfig().getEnabledGroups().contains(DATA));
              assertTrue(config.getNetworkConfig().getRestApiConfig().getEnabledGroups().contains(PERSISTENCE));
          },
          entry("hz.network.rest-api.enabled", "true"),
          entry("hz.network.rest-api.endpoint-groups.DATA.enabled", "true"),
          entry("hz.network.rest-api.endpoint-groups.hot_restart.enabled", "true"));
    }

    @Test
    public void shouldHandleRestApiConfigFromSysProperties_whenPersistenceEnabled() {
        runWithSystemProperties(() -> {
                    Config config = new Config();
                    new ExternalConfigurationOverride().overwriteMemberConfig(config);

                    assertTrue(config.getNetworkConfig().getRestApiConfig().isEnabled());
                    assertTrue(config.getNetworkConfig().getRestApiConfig().getEnabledGroups().contains(DATA));
                    assertTrue(config.getNetworkConfig().getRestApiConfig().getEnabledGroups().contains(PERSISTENCE));
                },
                entry("hz.network.rest-api.enabled", "true"),
                entry("hz.network.rest-api.endpoint-groups.DATA.enabled", "true"),
                entry("hz.network.rest-api.endpoint-groups.persistence.enabled", "true"));
    }

    @Test
    public void shouldHandleRestApiConfigFromSysProperties_when_bothPersistenceAndHotRestartAreEnabled() {
        runWithSystemProperties(() -> {
                    Config config = new Config();
                    new ExternalConfigurationOverride().overwriteMemberConfig(config);

                    assertTrue(config.getNetworkConfig().getRestApiConfig().isEnabled());
                    assertTrue(config.getNetworkConfig().getRestApiConfig().getEnabledGroups().contains(DATA));
                    assertTrue(config.getNetworkConfig().getRestApiConfig().getEnabledGroups().contains(PERSISTENCE));
                },
                entry("hz.network.rest-api.enabled", "true"),
                entry("hz.network.rest-api.endpoint-groups.DATA.enabled", "true"),
                entry("hz.network.rest-api.endpoint-groups.hot_restart.enabled", "true"),
                entry("hz.network.rest-api.endpoint-groups.persistence.enabled", "true"));
    }

    @Test(expected = InvalidConfigurationException.class)
    public void shouldHandleRestApiConfigFromSysPropertiesInvalidEntry() {
        runWithSystemProperties(() -> {
              Config config = new Config();
              new ExternalConfigurationOverride().overwriteMemberConfig(config);
          },
          entry("hz.network.rest-api.enabled", "true"),
          entry("hz.network.rest-api.endpoint-groups.fooo.enabled", "true"),
          entry("hz.network.rest-api.endpoint-groups.HOT_RESTART.enabled", "true"));
    }
}
