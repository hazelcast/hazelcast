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
import com.hazelcast.config.EventJournalConfig;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.InvalidConfigurationException;
import com.hazelcast.config.MerkleTreeConfig;
import com.hazelcast.config.NearCacheConfig;
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
import static com.hazelcast.config.RestEndpointGroup.*;
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
    public void shouldHandleCustomPropertiesConfig() throws Exception {
        Config config = new Config();

        withEnvironmentVariable("HZ_PROPERTIES_foo", "bar")
          .execute(() -> new ExternalConfigurationOverride().overwriteMemberConfig(config));

        assertEquals("bar", config.getProperty("foo"));
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
    public void shouldHandleScheduledServiceConfigMergePolicy() throws Exception {
        Config config = new Config();
        config.getScheduledExecutorConfig("foo1")
          .getMergePolicyConfig()
          .setPolicy("foo")
          .setBatchSize(4);

        withEnvironmentVariable("HZ_SCHEDULEDEXECUTORSERVICE_FOO1_MERGEPOLICY_CLASSNAME", "CustomMergePolicy")
          .execute(() -> new ExternalConfigurationOverride().overwriteMemberConfig(config));

        assertEquals("CustomMergePolicy", config.getScheduledExecutorConfig("foo1").getMergePolicyConfig().getPolicy());
        assertEquals(4, config.getScheduledExecutorConfig("foo1").getMergePolicyConfig().getBatchSize());
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
    public void shouldHandleCardinalityEstimatorConfigMergePolicy() throws Exception {
        Config config = new Config();
        config.getCardinalityEstimatorConfig("foo")
          .getMergePolicyConfig()
          .setPolicy("foo")
          .setBatchSize(4);

        withEnvironmentVariable("HZ_CARDINALITYESTIMATOR_FOO_MERGEPOLICY_CLASSNAME", "PutIfAbsentMergePolicy")
          .execute(() -> new ExternalConfigurationOverride().overwriteMemberConfig(config));

        assertEquals("PutIfAbsentMergePolicy", config.getCardinalityEstimatorConfig("foo").getMergePolicyConfig().getPolicy());
        assertEquals(4, config.getCardinalityEstimatorConfig("foo").getMergePolicyConfig().getBatchSize());
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
    public void shouldHandleMemcachedProtocolConfig() throws Exception {
        Config config = new Config();
        config.getNetworkConfig().getMemcacheProtocolConfig().setEnabled(false);

        withEnvironmentVariable("HZ_NETWORK_MEMCACHEPROTOCOL_ENABLED", "true")
          .execute(() -> new ExternalConfigurationOverride().overwriteMemberConfig(config));

        assertTrue(config.getNetworkConfig().getMemcacheProtocolConfig().isEnabled());
    }

    @Test
    public void shouldHandleRingBufferConfig() throws Exception {
        Config config = new Config();
        config.getRingbufferConfig("foo")
          .setAsyncBackupCount(2)
          .setBackupCount(4);

        withEnvironmentVariable("HZ_RINGBUFFER_FOO_ASYNCBACKUPCOUNT", "1")
          .execute(() -> new ExternalConfigurationOverride().overwriteMemberConfig(config));

        assertEquals(1, config.getRingbufferConfig("foo").getAsyncBackupCount());
        assertEquals(4, config.getRingbufferConfig("foo").getBackupCount());
    }

    @Test
    public void shouldHandleRingBufferConfigMergePolicy() throws Exception {
        Config config = new Config();
        config.getRingbufferConfig("foo")
          .getMergePolicyConfig()
          .setPolicy("foo")
          .setBatchSize(4);

        withEnvironmentVariable("HZ_RINGBUFFER_FOO_MERGEPOLICY_CLASSNAME", "PutIfAbsentMergePolicy")
          .execute(() -> new ExternalConfigurationOverride().overwriteMemberConfig(config));

        assertEquals("PutIfAbsentMergePolicy", config.getRingbufferConfig("foo").getMergePolicyConfig().getPolicy());
        assertEquals(4, config.getRingbufferConfig("foo").getMergePolicyConfig().getBatchSize());
    }

    @Test
    public void shouldHandleCacheConfig() throws Exception {
        Config config = new Config();
        config.getCacheConfig("foo")
          .setStatisticsEnabled(false)
          .setBackupCount(4);

        withEnvironmentVariable("HZ_CACHE_FOO_STATISTICSENABLED", "true")
          .execute(() -> new ExternalConfigurationOverride().overwriteMemberConfig(config));

        assertTrue(config.getCacheConfig("foo").isStatisticsEnabled());
        assertEquals(4, config.getCacheConfig("foo").getBackupCount());
    }

    @Test
    public void shouldHandleCacheConfigMergePolicy() throws Exception {
        Config config = new Config();
        config.getCacheConfig("foo")
          .getMergePolicyConfig()
          .setPolicy("foo")
          .setBatchSize(4);

        withEnvironmentVariable("HZ_CACHE_FOO_MERGEPOLICY_CLASSNAME", "PutIfAbsentMergePolicy")
          .execute(() -> new ExternalConfigurationOverride().overwriteMemberConfig(config));

        assertEquals("PutIfAbsentMergePolicy", config.getCacheConfig("foo").getMergePolicyConfig().getPolicy());
        assertEquals(4, config.getCacheConfig("foo").getMergePolicyConfig().getBatchSize());
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
    public void shouldHandleQueueConfig() throws Exception {
        Config config = new Config();
        config.getQueueConfig("foo")
          .setBackupCount(4)
          .setMaxSize(10);

        withEnvironmentVariable("HZ_QUEUE_FOO_BACKUPCOUNT", "2")
          .execute(() -> new ExternalConfigurationOverride().overwriteMemberConfig(config));

        assertEquals(2, config.getQueueConfig("foo").getBackupCount());
        assertEquals(10, config.getQueueConfig("foo").getMaxSize());
    }

    @Test
    public void shouldHandleQueueConfigMergePolicy() throws Exception {
        Config config = new Config();
        config.getQueueConfig("foo")
          .getMergePolicyConfig()
          .setPolicy("foo")
          .setBatchSize(4);

        withEnvironmentVariable("HZ_QUEUE_FOO_MERGEPOLICY_CLASSNAME", "PutIfAbsentMergePolicy")
          .execute(() -> new ExternalConfigurationOverride().overwriteMemberConfig(config));

        assertEquals("PutIfAbsentMergePolicy", config.getQueueConfig("foo").getMergePolicyConfig().getPolicy());
        assertEquals(4, config.getQueueConfig("foo").getMergePolicyConfig().getBatchSize());
    }

    @Test
    public void shouldHandleListConfig() throws Exception {
        Config config = new Config();
        config.getListConfig("foo")
          .setBackupCount(4)
          .setMaxSize(10);

        withEnvironmentVariable("HZ_LIST_FOO_BACKUPCOUNT", "2")
          .execute(() -> new ExternalConfigurationOverride().overwriteMemberConfig(config));

        assertEquals(2, config.getListConfig("foo").getBackupCount());
        assertEquals(10, config.getListConfig("foo").getMaxSize());
    }

    @Test
    public void shouldHandleListConfigMergePolicy() throws Exception {
        Config config = new Config();
        config.getListConfig("foo")
          .getMergePolicyConfig()
          .setPolicy("foo")
          .setBatchSize(4);

        withEnvironmentVariable("HZ_LIST_FOO_MERGEPOLICY_CLASSNAME", "PutIfAbsentMergePolicy")
          .execute(() -> new ExternalConfigurationOverride().overwriteMemberConfig(config));

        assertEquals("PutIfAbsentMergePolicy", config.getListConfig("foo").getMergePolicyConfig().getPolicy());
        assertEquals(4, config.getListConfig("foo").getMergePolicyConfig().getBatchSize());
    }

    @Test
    public void shouldHandleSetConfig() throws Exception {
        Config config = new Config();
        config.getSetConfig("foo")
          .setBackupCount(4)
          .setMaxSize(10);

        withEnvironmentVariable("HZ_SET_FOO_BACKUPCOUNT", "2")
          .execute(() -> new ExternalConfigurationOverride().overwriteMemberConfig(config));

        assertEquals(2, config.getSetConfig("foo").getBackupCount());
        assertEquals(10, config.getSetConfig("foo").getMaxSize());
    }

    @Test
    public void shouldHandleSetConfigMergePolicy() throws Exception {
        Config config = new Config();
        config.getSetConfig("foo")
          .getMergePolicyConfig()
          .setPolicy("foo")
          .setBatchSize(4);

        withEnvironmentVariable("HZ_SET_FOO_MERGEPOLICY_CLASSNAME", "PutIfAbsentMergePolicy")
          .execute(() -> new ExternalConfigurationOverride().overwriteMemberConfig(config));

        assertEquals("PutIfAbsentMergePolicy", config.getSetConfig("foo").getMergePolicyConfig().getPolicy());
        assertEquals(4, config.getSetConfig("foo").getMergePolicyConfig().getBatchSize());
    }

    @Test
    public void shouldHandleMapConfig() throws Exception {
        Config config = new Config();
        config.getMapConfig("foo")
          .setBackupCount(4)
          .setMaxIdleSeconds(100);

        withEnvironmentVariable("HZ_MAP_FOO_BACKUPCOUNT", "2")
          .execute(() -> new ExternalConfigurationOverride().overwriteMemberConfig(config));

        assertEquals(2, config.getMapConfig("foo").getBackupCount());
        assertEquals(100, config.getMapConfig("foo").getMaxIdleSeconds());
    }

    @Test
    public void shouldHandleMapConfigMergePolicy() throws Exception {
        Config config = new Config();
        config.getMapConfig("foo")
          .getMergePolicyConfig()
          .setPolicy("foo")
          .setBatchSize(4);

        withEnvironmentVariable("HZ_MAP_FOO_MERGEPOLICY_CLASSNAME", "PutIfAbsentMergePolicy")
          .execute(() -> new ExternalConfigurationOverride().overwriteMemberConfig(config));

        assertEquals("PutIfAbsentMergePolicy", config.getMapConfig("foo").getMergePolicyConfig().getPolicy());
        assertEquals(4, config.getMapConfig("foo").getMergePolicyConfig().getBatchSize());
    }

    @Test
    public void shouldHandleMapConfigNearCache() throws Exception {
        Config config = new Config();
        NearCacheConfig nearCacheConfig = new NearCacheConfig()
          .setMaxIdleSeconds(10)
          .setInMemoryFormat(InMemoryFormat.NATIVE);
        config.getMapConfig("foo").setNearCacheConfig(nearCacheConfig);


        withEnvironmentVariable("HZ_MAP_FOO_NEARCACHE_MAXIDLESECONDS", "5")
          .execute(() -> new ExternalConfigurationOverride().overwriteMemberConfig(config));

        assertEquals(5, config.getMapConfig("foo").getNearCacheConfig().getMaxIdleSeconds());
        assertEquals(InMemoryFormat.NATIVE, config.getMapConfig("foo").getNearCacheConfig().getInMemoryFormat());
    }

    @Test
    public void shouldHandleMapMerkleTreeConfig() throws Exception {
        Config config = new Config();
        MerkleTreeConfig merkleTreeConfig = new MerkleTreeConfig()
          .setEnabled(false)
          .setDepth(5);

        config.getMapConfig("foo1")
          .setBackupCount(4)
          .setMerkleTreeConfig(merkleTreeConfig);

        withEnvironmentVariable("HZ_MAP_FOO1_BACKUPCOUNT", "2")
          .and("HZ_MAP_FOO1_MERKLETREE_ENABLED", "true")
          .execute(() -> new ExternalConfigurationOverride().overwriteMemberConfig(config));

        assertEquals(2, config.getMapConfig("foo1").getBackupCount());
        assertTrue(config.getMapConfig("foo1").getMerkleTreeConfig().isEnabled());
        assertEquals(5, config.getMapConfig("foo1").getMerkleTreeConfig().getDepth());
    }

    @Test
    public void shouldHandleMapEventJournalConfig() throws Exception {
        Config config = new Config();
        EventJournalConfig eventJournalConfig = new EventJournalConfig()
          .setEnabled(false)
          .setCapacity(10);

        config.getMapConfig("foo1")
          .setBackupCount(4)
          .setEventJournalConfig(eventJournalConfig);

        withEnvironmentVariable("HZ_MAP_FOO1_BACKUPCOUNT", "2")
          .and("HZ_MAP_FOO1_EVENTJOURNAL_ENABLED", "true")
          .execute(() -> new ExternalConfigurationOverride().overwriteMemberConfig(config));

        assertEquals(2, config.getMapConfig("foo1").getBackupCount());
        assertTrue(config.getMapConfig("foo1").getEventJournalConfig().isEnabled());
        assertEquals(10, config.getMapConfig("foo1").getEventJournalConfig().getCapacity());
    }

    @Test
    public void shouldHandleMapMapStoreConfig() throws Exception {
        Config config = new Config();
        config.getMapConfig("foo")
          .getMapStoreConfig()
          .setEnabled(true)
          .setWriteBatchSize(10);

        withEnvironmentVariable("HZ_MAP_FOO_MAPSTORE_ENABLED", "true")
          .execute(() -> new ExternalConfigurationOverride().overwriteMemberConfig(config));

        assertTrue(config.getMapConfig("foo").getMapStoreConfig().isEnabled());
        assertEquals(10, config.getMapConfig("foo").getMapStoreConfig().getWriteBatchSize());
    }

    @Test
    public void shouldHandleReplicatedMapConfig() throws Exception {
        Config config = new Config();
        config.getReplicatedMapConfig("foo")
          .setAsyncFillup(false)
          .setStatisticsEnabled(false);

        withEnvironmentVariable("HZ_REPLICATEDMAP_FOO_ASYNCFILLUP", "true")
          .execute(() -> new ExternalConfigurationOverride().overwriteMemberConfig(config));

        assertTrue(config.getReplicatedMapConfig("foo").isAsyncFillup());
        assertFalse(config.getReplicatedMapConfig("foo").isStatisticsEnabled());
    }

    @Test
    public void shouldHandleReplicatedMapConfigMergePolicy() throws Exception {
        Config config = new Config();
        config.getReplicatedMapConfig("foo")
          .getMergePolicyConfig()
          .setPolicy("foo")
          .setBatchSize(4);

        withEnvironmentVariable("HZ_REPLICATEDMAP_FOO_MERGEPOLICY_CLASSNAME", "PutIfAbsentMergePolicy")
          .execute(() -> new ExternalConfigurationOverride().overwriteMemberConfig(config));

        assertEquals("PutIfAbsentMergePolicy", config.getReplicatedMapConfig("foo").getMergePolicyConfig().getPolicy());
        assertEquals(4, config.getReplicatedMapConfig("foo").getMergePolicyConfig().getBatchSize());
    }

    @Test
    public void shouldHandleMultiMapConfig() throws Exception {
        Config config = new Config();
        config.getMultiMapConfig("foo")
          .setBackupCount(4)
          .setBinary(false);

        withEnvironmentVariable("HZ_MULTIMAP_FOO_BACKUPCOUNT", "2")
          .execute(() -> new ExternalConfigurationOverride().overwriteMemberConfig(config));

        assertEquals(2, config.getMultiMapConfig("foo").getBackupCount());
        assertFalse(config.getMultiMapConfig("foo").isBinary());
    }

    @Test
    public void shouldHandleMultiMapConfigMergePolicy() throws Exception {
        Config config = new Config();
        config.getMultiMapConfig("foo")
          .getMergePolicyConfig()
          .setPolicy("foo")
          .setBatchSize(4);

        withEnvironmentVariable("HZ_MULTIMAP_FOO_MERGEPOLICY_CLASSNAME", "PutIfAbsentMergePolicy")
          .execute(() -> new ExternalConfigurationOverride().overwriteMemberConfig(config));

        assertEquals("PutIfAbsentMergePolicy", config.getMultiMapConfig("foo").getMergePolicyConfig().getPolicy());
        assertEquals(4, config.getMultiMapConfig("foo").getMergePolicyConfig().getBatchSize());
    }

    @Test
    public void shouldHandleAuditLogConfig() throws Exception {
        Config config = new Config();
        config.getAuditlogConfig()
          .setEnabled(false)
          .setFactoryClassName("com.acme.AuditlogToSyslogFactory")
          .setProperty("host", "syslogserver.acme.com")
          .setProperty("port", "514")
          .setProperty("type", "tcp");

        withEnvironmentVariable("HZ_AUDITLOG_ENABLED", "true")
          .execute(() -> new ExternalConfigurationOverride().overwriteMemberConfig(config));

        assertTrue(config.getAuditlogConfig().isEnabled());
        assertEquals("com.acme.AuditlogToSyslogFactory", config.getAuditlogConfig().getFactoryClassName());
        assertEquals("syslogserver.acme.com", config.getAuditlogConfig().getProperty("host"));
        assertEquals("514", config.getAuditlogConfig().getProperty("port"));
        assertEquals("tcp", config.getAuditlogConfig().getProperty("type"));
    }

    @Test
    public void shouldHandleRestApiConfig() throws Exception {
        Config config = new Config();

        withEnvironmentVariable("HZ_NETWORK_RESTAPI_ENABLED", "true")
          .and("HZ_NETWORK_RESTAPI_ENDPOINTGROUPS_DATA_ENABLED", "true")
          .and("HZ_NETWORK_RESTAPI_ENDPOINTGROUPS_HOTRESTART_ENABLED", "true")
          .execute(() -> new ExternalConfigurationOverride().overwriteMemberConfig(config));
        System.out.println(config.getNetworkConfig().getRestApiConfig());
        assertTrue(config.getNetworkConfig().getRestApiConfig().isEnabled());
        assertTrue(config.getNetworkConfig().getRestApiConfig().getEnabledGroups().contains(DATA));
        assertTrue(config.getNetworkConfig().getRestApiConfig().getEnabledGroups().contains(PERSISTENCE));
    }

    @Test
    public void shouldHandleRestApiConfig_whenPersistenceEnabled() throws Exception {
        Config config = new Config();

        withEnvironmentVariable("HZ_NETWORK_RESTAPI_ENABLED", "true")
                .and("HZ_NETWORK_RESTAPI_ENDPOINTGROUPS_DATA_ENABLED", "true")
                .and("HZ_NETWORK_RESTAPI_ENDPOINTGROUPS_PERSISTENCE_ENABLED", "true")
                .execute(() -> new ExternalConfigurationOverride().overwriteMemberConfig(config));
        System.out.println(config.getNetworkConfig().getRestApiConfig());
        assertTrue(config.getNetworkConfig().getRestApiConfig().isEnabled());
        assertTrue(config.getNetworkConfig().getRestApiConfig().getEnabledGroups().contains(DATA));
        assertTrue(config.getNetworkConfig().getRestApiConfig().getEnabledGroups().contains(PERSISTENCE));
    }


    @Test
    public void shouldHandleRestApiConfig_when_bothPersistenceAndHotRestartAreEnabled() throws Exception {
        Config config = new Config();

        withEnvironmentVariable("HZ_NETWORK_RESTAPI_ENABLED", "true")
                .and("HZ_NETWORK_RESTAPI_ENDPOINTGROUPS_DATA_ENABLED", "true")
                .and("HZ_NETWORK_RESTAPI_ENDPOINTGROUPS_HOTRESTART_ENABLED", "true")
                .and("HZ_NETWORK_RESTAPI_ENDPOINTGROUPS_PERSISTENCE_ENABLED", "true")
                .execute(() -> new ExternalConfigurationOverride().overwriteMemberConfig(config));
        System.out.println(config.getNetworkConfig().getRestApiConfig());
        assertTrue(config.getNetworkConfig().getRestApiConfig().isEnabled());
        assertTrue(config.getNetworkConfig().getRestApiConfig().getEnabledGroups().contains(DATA));
        assertTrue(config.getNetworkConfig().getRestApiConfig().getEnabledGroups().contains(PERSISTENCE));
    }

    @Test(expected = InvalidConfigurationException.class)
    public void shouldHandleRestApiConfigInvalidEntry() throws Exception {
        Config config = new Config();

        withEnvironmentVariable("HZ_NETWORK_RESTAPI_ENABLED", "true")
          .and("HZ_NETWORK_RESTAPI_ENDPOINTGROUPS_FOO_ENABLED", "true")
          .execute(() -> new ExternalConfigurationOverride().overwriteMemberConfig(config));
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
