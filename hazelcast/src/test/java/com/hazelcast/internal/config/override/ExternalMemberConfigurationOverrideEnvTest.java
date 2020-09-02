/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.EnvironmentVariables;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.internal.config.override.ExternalConfigTestUtils.runWithSystemProperty;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class ExternalMemberConfigurationOverrideEnvTest extends HazelcastTestSupport {

    @ClassRule
    public static final EnvironmentVariables environmentVariables = new EnvironmentVariables();

    @Test
    public void shouldExtractConfigFromEnv() {
        environmentVariables
          .set("HZ_CLUSTERNAME", "test")
          .set("HZ_METRICS_ENABLED", "false")
          .set("HZ_NETWORK_JOIN_AUTODETECTION_ENABLED", "false")
          .set("HZ_CACHE_DEFAULT_KEYTYPE_CLASSNAME", "java.lang.Object")
          .set("HZ_EXECUTORSERVICE_CUSTOM_POOLSIZE", "42")
          .set("HZ_EXECUTORSERVICE_DEFAULT_STATISTICSENABLED", "false")
          .set("HZ_DURABLEEXECUTORSERVICE_DEFAULT_CAPACITY", "42")
          .set("HZ_SCHEDULEDEXECUTORSERVICE_DEFAULT_CAPACITY", "40")
          .set("HZ_QUEUE_DEFAULT_MAXSIZE", "2");

        Config config = new Config();
        new ExternalConfigurationOverride().overwriteMemberConfig(config);

        assertEquals("test", config.getClusterName());
        assertFalse(config.getMetricsConfig().isEnabled());
        assertEquals(42, config.getExecutorConfig("custom").getPoolSize());
        assertEquals("java.lang.Object", config.getCacheConfig("default").getKeyType());
        assertFalse(config.getExecutorConfig("default").isStatisticsEnabled());
        assertEquals(42, config.getDurableExecutorConfig("default").getCapacity());
        assertEquals(40, config.getScheduledExecutorConfig("default").getCapacity());
        assertEquals(2, config.getQueueConfig("default").getMaxSize());
        assertFalse(config.getNetworkConfig().getJoin().isAutoDetectionEnabled());
    }

    @Test(expected = InvalidConfigurationException.class)
    public void shouldDisallowConflictingEntries() {
        environmentVariables
          .set("HZ_CLUSTERNAME", "test");

        runWithSystemProperty("hz.cluster-name", "test2", () -> {
            Config config = new Config();
            new ExternalConfigurationOverride().overwriteMemberConfig(config);
        });
    }
}
