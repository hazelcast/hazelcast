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
import com.hazelcast.config.RestServerEndpointConfig;
import com.hazelcast.config.ServerSocketEndpointConfig;
import com.hazelcast.config.UserCodeDeploymentConfig;
import com.hazelcast.instance.EndpointQualifier;
import com.hazelcast.splitbrainprotection.SplitBrainProtectionOn;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.github.stefanbirkner.systemlambda.SystemLambda.withEnvironmentVariable;
import static com.hazelcast.internal.config.override.ExternalConfigTestUtils.runWithSystemProperty;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class ExternalMemberConfigurationOverrideEnvTest extends HazelcastTestSupport {

    @Test
    public void shouldExtractConfigFromEnv() throws Exception {
        Config config = new Config();
        withEnvironmentVariable("HZ_CLUSTERNAME", "test")
          .and("HZ_METRICS_ENABLED", "false")
          .and("HZ_NETWORK_JOIN_AUTODETECTION_ENABLED", "false")
          .and("HZ_CACHE_DEFAULT_KEYTYPE_CLASSNAME", "java.lang.Object2")
          .and("HZ_EXECUTORSERVICE_CUSTOM_POOLSIZE", "42")
          .and("HZ_EXECUTORSERVICE_DEFAULT_STATISTICSENABLED", "false")
          .and("HZ_DURABLEEXECUTORSERVICE_DEFAULT_CAPACITY", "42")
          .and("HZ_SCHEDULEDEXECUTORSERVICE_DEFAULT_CAPACITY", "40")
          .and("HZ_QUEUE_DEFAULT_MAXSIZE", "2")
          .execute(() -> new ExternalConfigurationOverride().overwriteMemberConfig(config));

        assertEquals("test", config.getClusterName());
        assertFalse(config.getMetricsConfig().isEnabled());
        assertEquals(42, config.getExecutorConfig("custom").getPoolSize());
        assertEquals("java.lang.Object2", config.getCacheConfig("default").getKeyType());
        assertFalse(config.getExecutorConfig("default").isStatisticsEnabled());
        assertEquals(42, config.getDurableExecutorConfig("default").getCapacity());
        assertEquals(40, config.getScheduledExecutorConfig("default").getCapacity());
        assertEquals(2, config.getQueueConfig("default").getMaxSize());
        assertFalse(config.getNetworkConfig().getJoin().isAutoDetectionEnabled());
    }

    @Test
    public void shouldHandleAdvancedNetworkEndpointConfiguration() throws Exception {
        Config config = new Config();
        config.getAdvancedNetworkConfig().setClientEndpointConfig(new ServerSocketEndpointConfig()
          .setPort(9000)
          .setPublicAddress("172.29.1.1"));
        config.getAdvancedNetworkConfig().setMemberEndpointConfig(new ServerSocketEndpointConfig()
          .setPort(9001)
          .setPublicAddress("172.29.1.1"));
        config.getAdvancedNetworkConfig().setRestEndpointConfig(new RestServerEndpointConfig()
          .setPort(9002)
          .setPublicAddress("172.29.1.1"));
        config.getAdvancedNetworkConfig().setMemcacheEndpointConfig(new ServerSocketEndpointConfig()
          .setPort(9003)
          .setPublicAddress("172.29.1.1"));

        withEnvironmentVariable("HZ_ADVANCEDNETWORK_CLIENTSERVERSOCKETENDPOINTCONFIG.PUBLICADDRESS", "127.0.0.1")
          .and("HZ_ADVANCEDNETWORK_MEMBERSERVERSOCKETENDPOINTCONFIG.PUBLICADDRESS", "127.0.0.2")
          .and("HZ_ADVANCEDNETWORK_RESTSERVERSOCKETENDPOINTCONFIG.PUBLICADDRESS", "127.0.0.3")
          .and("HZ_ADVANCEDNETWORK_MEMCACHESERVERSOCKETENDPOINTCONFIG.PUBLICADDRESS", "127.0.0.4")
          .execute(() -> new ExternalConfigurationOverride().overwriteMemberConfig(config));

        ServerSocketEndpointConfig clientEndpointConfig = (ServerSocketEndpointConfig) config.getAdvancedNetworkConfig().getEndpointConfigs().get(EndpointQualifier.CLIENT);
        ServerSocketEndpointConfig memberEndpointConfig = (ServerSocketEndpointConfig) config.getAdvancedNetworkConfig().getEndpointConfigs().get(EndpointQualifier.MEMBER);
        ServerSocketEndpointConfig restEndpointConfig = (ServerSocketEndpointConfig) config.getAdvancedNetworkConfig().getEndpointConfigs().get(EndpointQualifier.REST);
        ServerSocketEndpointConfig memcacheEndpointConfig = (ServerSocketEndpointConfig) config.getAdvancedNetworkConfig().getEndpointConfigs().get(EndpointQualifier.MEMCACHE);

        assertEquals(9000, clientEndpointConfig.getPort());
        assertEquals("127.0.0.1", clientEndpointConfig.getPublicAddress());
        assertEquals(9001, memberEndpointConfig.getPort());
        assertEquals("127.0.0.2", memberEndpointConfig.getPublicAddress());
        assertEquals(9002, restEndpointConfig.getPort());
        assertEquals("127.0.0.3", restEndpointConfig.getPublicAddress());
        assertEquals(9003, memcacheEndpointConfig.getPort());
        assertEquals("127.0.0.4", memcacheEndpointConfig.getPublicAddress());
    }

    @Test
    public void shouldHandleNetworkRestApiConfig() throws Exception {
        Config config = new Config();
        config.getNetworkConfig()
          .getRestApiConfig()
          .disableAllGroups();

        withEnvironmentVariable("HZ_NETWORK_RESTAPI_ENABLED", "true")
          .execute(() -> new ExternalConfigurationOverride().overwriteMemberConfig(config));

        assertTrue(config.getNetworkConfig().getRestApiConfig().getEnabledGroups().isEmpty());
    }

    @Test
    public void shouldHandleHotRestartPersistenceConfig() throws Exception {
        Config config = new Config();
        config.getHotRestartPersistenceConfig()
          .setEnabled(true)
          .setParallelism(4);

        withEnvironmentVariable("HZ_HOTRESTARTPERSISTENCE_ENABLED", "true")
          .execute(() -> new ExternalConfigurationOverride().overwriteMemberConfig(config));

        assertTrue(config.getHotRestartPersistenceConfig().isEnabled());
        assertEquals(4, config.getHotRestartPersistenceConfig().getParallelism());
    }

    @Test
    public void shouldHandleUserCodeDeploymentConfig() throws Exception {
        Config config = new Config();
        config.getUserCodeDeploymentConfig()
          .setEnabled(true)
          .setClassCacheMode(UserCodeDeploymentConfig.ClassCacheMode.OFF);

        withEnvironmentVariable("HZ_USERCODEDEPLOYMENT_ENABLED", "true")
          .execute(() -> new ExternalConfigurationOverride().overwriteMemberConfig(config));

        assertTrue(config.getUserCodeDeploymentConfig().isEnabled());
        assertEquals(UserCodeDeploymentConfig.ClassCacheMode.OFF, config.getUserCodeDeploymentConfig().getClassCacheMode());
    }

    @Test
    public void shouldHandleExecutorServiceConfig() throws Exception {
        Config config = new Config();
        config.getExecutorConfig("foo1")
          .setPoolSize(1)
          .setStatisticsEnabled(true);

        withEnvironmentVariable("HZ_EXECUTORSERVICE_FOO1_STATISTICSENABLED", "true")
          .and("HZ_EXECUTORSERVICE_FOO1_QUEUECAPACITY", "17")
          .execute(() -> new ExternalConfigurationOverride().overwriteMemberConfig(config));

        assertTrue(config.getExecutorConfig("foo1").isStatisticsEnabled());
        assertEquals(17, config.getExecutorConfig("foo1").getQueueCapacity());
        assertEquals(1, config.getExecutorConfig("foo1").getPoolSize());
    }

    @Test
    public void shouldHandleDurableExecutorServiceConfig() throws Exception {
        Config config = new Config();
        config.getDurableExecutorConfig("foo1")
          .setPoolSize(1)
          .setStatisticsEnabled(true);

        withEnvironmentVariable("HZ_DURABLEEXECUTORSERVICE_FOO1_STATISTICSENABLED", "true")
          .and("HZ_DURABLEEXECUTORSERVICE_FOO1_CAPACITY", "17")
          .execute(() -> new ExternalConfigurationOverride().overwriteMemberConfig(config));

        assertTrue(config.getDurableExecutorConfig("foo1").isStatisticsEnabled());
        assertEquals(17, config.getDurableExecutorConfig("foo1").getCapacity());
        assertEquals(1, config.getDurableExecutorConfig("foo1").getPoolSize());
    }

    @Test
    public void shouldHandleScheduledServiceConfig() throws Exception {
        Config config = new Config();
        config.getScheduledExecutorConfig("foo1")
          .setPoolSize(1)
          .setStatisticsEnabled(true);

        withEnvironmentVariable("HZ_SCHEDULEDEXECUTORSERVICE_FOO1_ENABLED", "true")
          .and("HZ_SCHEDULEDEXECUTORSERVICE_FOO1_CAPACITY", "17")
          .execute(() -> new ExternalConfigurationOverride().overwriteMemberConfig(config));

        assertTrue(config.getScheduledExecutorConfig("foo1").isStatisticsEnabled());
        assertEquals(17, config.getScheduledExecutorConfig("foo1").getCapacity());
        assertEquals(1, config.getScheduledExecutorConfig("foo1").getPoolSize());
    }

    @Test
    public void shouldHandleCardinalityEstimatorConfig() throws Exception {
        Config config = new Config();
        config.getCardinalityEstimatorConfig("foo")
          .setAsyncBackupCount(4);

        withEnvironmentVariable("HZ_CARDINALITYESTIMATOR_FOO_BACKUPCOUNT", "2")
          .execute(() -> new ExternalConfigurationOverride().overwriteMemberConfig(config));

        assertEquals(2,  config.getCardinalityEstimatorConfig("foo").getBackupCount());
        assertEquals(4,  config.getCardinalityEstimatorConfig("foo").getAsyncBackupCount());
    }

    @Test
    public void shouldHandleSplitBrainProtectionConfig() throws Exception {
        Config config = new Config();
        config.getSplitBrainProtectionConfig("foo")
          .setEnabled(true)
          .setProtectOn(SplitBrainProtectionOn.READ);

        withEnvironmentVariable("HZ_SPLITBRAINPROTECTION_FOO_ENABLED", "true")
          .and("HZ_SPLITBRAINPROTECTION_FOO_FUNCTIONCLASSNAME", "com.foo.SomeClass")
          .execute(() -> new ExternalConfigurationOverride().overwriteMemberConfig(config));

        assertTrue(config.getSplitBrainProtectionConfig("foo").isEnabled());
        assertSame(SplitBrainProtectionOn.READ, config.getSplitBrainProtectionConfig("foo").getProtectOn());
        assertEquals("com.foo.SomeClass", config.getSplitBrainProtectionConfig("foo").getFunctionClassName());
    }

    @Test
    public void shouldHandlePNCounterConfig() throws Exception {
        Config config = new Config();
        config.getPNCounterConfig("foo")
          .setStatisticsEnabled(false)
          .setReplicaCount(2);

        withEnvironmentVariable("HZ_PNCOUNTER_FOO_STATISTICSENABLED", "true")
          .execute(() -> new ExternalConfigurationOverride().overwriteMemberConfig(config));

        assertTrue(config.getPNCounterConfig("foo").isStatisticsEnabled());
        assertEquals(2, config.getPNCounterConfig("foo").getReplicaCount());
    }

    @Test
    public void shouldHandleFlakeIdConfig() throws Exception {
        Config config = new Config();
        config.getFlakeIdGeneratorConfig("foo")
          .setStatisticsEnabled(false)
          .setAllowedFutureMillis(1000);

        withEnvironmentVariable("HZ_FLAKEIDGENERATOR_FOO_STATISTICSENABLED", "true")
          .execute(() -> new ExternalConfigurationOverride().overwriteMemberConfig(config));

        assertTrue(config.getFlakeIdGeneratorConfig("foo").isStatisticsEnabled());
        assertEquals(1000, config.getFlakeIdGeneratorConfig("foo").getAllowedFutureMillis());
    }

    @Test
    public void shouldHandleMemcachedProtocolConfig() throws Exception {
        Config config = new Config();
        config.getNetworkConfig().getMemcacheProtocolConfig().setEnabled(false);

        withEnvironmentVariable("HZ_NETWORK_MEMCACHEPROTOCOL_ENABLED", "true")
          .execute(() -> new ExternalConfigurationOverride().overwriteMemberConfig(config));

        assertTrue(config.getNetworkConfig().getMemcacheProtocolConfig().isEnabled());
    }

    @Test(expected = InvalidConfigurationException.class)
    public void shouldDisallowConflictingEntries() throws Exception {
        withEnvironmentVariable("HZ_CLUSTERNAME", "test")
          .execute(
            () -> runWithSystemProperty("hz.cluster-name", "test2", () -> {
                Config config = new Config();
                new ExternalConfigurationOverride().overwriteMemberConfig(config);
            })
          );
    }
}
