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

package com.hazelcast.spring.context;

import com.hazelcast.config.CachePartitionLostListenerConfig;
import com.hazelcast.config.CacheSimpleConfig;
import com.hazelcast.config.CacheSimpleConfig.ExpiryPolicyFactoryConfig;
import com.hazelcast.config.CacheSimpleConfig.ExpiryPolicyFactoryConfig.DurationConfig;
import com.hazelcast.config.CacheSimpleConfig.ExpiryPolicyFactoryConfig.TimedExpiryPolicyFactoryConfig;
import com.hazelcast.config.CacheSimpleConfig.ExpiryPolicyFactoryConfig.TimedExpiryPolicyFactoryConfig.ExpiryPolicyType;
import com.hazelcast.config.Config;
import com.hazelcast.config.EvictionConfig;
import com.hazelcast.config.EvictionPolicy;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MaxSizePolicy;
import com.hazelcast.config.MergePolicyConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.spring.CustomSpringJUnit4ClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.test.context.ContextConfiguration;

import javax.annotation.Resource;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Tests for JCache parser.
 */
@RunWith(CustomSpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = {"test-jcache-application-context.xml"})
@Category(QuickTest.class)
public class TestJCache {

    @Autowired
    private ApplicationContext context;

    @Resource(name = "instance1")
    private HazelcastInstance instance1;

    @BeforeClass
    @AfterClass
    public static void tearDown() {
        Hazelcast.shutdownAll();
    }

    @Test
    public void testContextInitializedSuccessfully() {
        assertNotNull(context);
    }

    @Test
    public void testConfig() {
        assertNotNull(instance1);

        CacheSimpleConfig simpleConfig = instance1.getConfig().getCacheConfigs().get("cache1");

        assertNotNull(simpleConfig);

        assertEquals(1, simpleConfig.getAsyncBackupCount());
        assertEquals(2, simpleConfig.getBackupCount());
        assertEquals("java.lang.Integer", simpleConfig.getKeyType());
        assertEquals("java.lang.String", simpleConfig.getValueType());
        assertTrue(simpleConfig.isStatisticsEnabled());
        assertTrue(simpleConfig.isManagementEnabled());
        assertTrue(simpleConfig.isReadThrough());
        assertTrue(simpleConfig.isWriteThrough());
        assertEquals("com.hazelcast.cache.MyCacheLoaderFactory", simpleConfig.getCacheLoaderFactory());
        assertEquals("com.hazelcast.cache.MyCacheWriterFactory", simpleConfig.getCacheWriterFactory());
        assertEquals("com.hazelcast.cache.MyExpiryPolicyFactory",
                simpleConfig.getExpiryPolicyFactoryConfig().getClassName());
        assertEquals(InMemoryFormat.OBJECT, simpleConfig.getInMemoryFormat());
        assertNotNull(simpleConfig.getEvictionConfig());
        assertEquals(50, simpleConfig.getEvictionConfig().getSize());
        assertEquals(MaxSizePolicy.ENTRY_COUNT,
                simpleConfig.getEvictionConfig().getMaxSizePolicy());
        assertEquals(EvictionPolicy.LRU, simpleConfig.getEvictionConfig().getEvictionPolicy());
    }

    @Test
    public void cacheConfigXmlTest_TimedCreatedExpiryPolicyFactory() {
        Config config = instance1.getConfig();

        CacheSimpleConfig cacheWithTimedCreatedExpiryPolicyFactoryConfig =
                config.getCacheConfig("cacheWithTimedCreatedExpiryPolicyFactory");
        ExpiryPolicyFactoryConfig expiryPolicyFactoryConfig =
                cacheWithTimedCreatedExpiryPolicyFactoryConfig.getExpiryPolicyFactoryConfig();
        ExpiryPolicyFactoryConfig.TimedExpiryPolicyFactoryConfig timedExpiryPolicyFactoryConfig =
                expiryPolicyFactoryConfig.getTimedExpiryPolicyFactoryConfig();
        DurationConfig durationConfig = timedExpiryPolicyFactoryConfig.getDurationConfig();

        assertNotNull(expiryPolicyFactoryConfig);
        assertNotNull(timedExpiryPolicyFactoryConfig);
        assertNotNull(durationConfig);
        assertNull(expiryPolicyFactoryConfig.getClassName());

        assertEquals(ExpiryPolicyType.CREATED, timedExpiryPolicyFactoryConfig.getExpiryPolicyType());
        assertEquals(1, durationConfig.getDurationAmount());
        assertEquals(TimeUnit.DAYS, durationConfig.getTimeUnit());
    }

    @Test
    public void cacheConfigXmlTest_TimedAccessedExpiryPolicyFactory() {
        Config config = instance1.getConfig();

        CacheSimpleConfig cacheWithTimedAccessedExpiryPolicyFactoryConfig =
                config.getCacheConfig("cacheWithTimedAccessedExpiryPolicyFactory");
        ExpiryPolicyFactoryConfig expiryPolicyFactoryConfig =
                cacheWithTimedAccessedExpiryPolicyFactoryConfig.getExpiryPolicyFactoryConfig();
        TimedExpiryPolicyFactoryConfig timedExpiryPolicyFactoryConfig =
                expiryPolicyFactoryConfig.getTimedExpiryPolicyFactoryConfig();
        DurationConfig durationConfig = timedExpiryPolicyFactoryConfig.getDurationConfig();

        assertNotNull(expiryPolicyFactoryConfig);
        assertNotNull(timedExpiryPolicyFactoryConfig);
        assertNotNull(durationConfig);
        assertNull(expiryPolicyFactoryConfig.getClassName());

        assertEquals(ExpiryPolicyType.ACCESSED, timedExpiryPolicyFactoryConfig.getExpiryPolicyType());
        assertEquals(2, durationConfig.getDurationAmount());
        assertEquals(TimeUnit.HOURS, durationConfig.getTimeUnit());
    }

    @Test
    public void cacheConfigXmlTest_TimedModifiedExpiryPolicyFactory() {
        Config config = instance1.getConfig();

        CacheSimpleConfig cacheWithTimedModifiedExpiryPolicyFactoryConfig =
                config.getCacheConfig("cacheWithTimedModifiedExpiryPolicyFactory");
        ExpiryPolicyFactoryConfig expiryPolicyFactoryConfig =
                cacheWithTimedModifiedExpiryPolicyFactoryConfig.getExpiryPolicyFactoryConfig();
        TimedExpiryPolicyFactoryConfig timedExpiryPolicyFactoryConfig =
                expiryPolicyFactoryConfig.getTimedExpiryPolicyFactoryConfig();
        DurationConfig durationConfig = timedExpiryPolicyFactoryConfig.getDurationConfig();

        assertNotNull(expiryPolicyFactoryConfig);
        assertNotNull(timedExpiryPolicyFactoryConfig);
        assertNotNull(durationConfig);
        assertNull(expiryPolicyFactoryConfig.getClassName());

        assertEquals(ExpiryPolicyType.MODIFIED, timedExpiryPolicyFactoryConfig.getExpiryPolicyType());
        assertEquals(3, durationConfig.getDurationAmount());
        assertEquals(TimeUnit.MINUTES, durationConfig.getTimeUnit());
    }

    @Test
    public void cacheConfigXmlTest_TimedModifiedTouchedPolicyFactory() {
        Config config = instance1.getConfig();

        CacheSimpleConfig cacheWithTimedTouchedExpiryPolicyFactoryConfig =
                config.getCacheConfig("cacheWithTimedTouchedExpiryPolicyFactory");
        ExpiryPolicyFactoryConfig expiryPolicyFactoryConfig =
                cacheWithTimedTouchedExpiryPolicyFactoryConfig.getExpiryPolicyFactoryConfig();
        TimedExpiryPolicyFactoryConfig timedExpiryPolicyFactoryConfig =
                expiryPolicyFactoryConfig.getTimedExpiryPolicyFactoryConfig();
        DurationConfig durationConfig = timedExpiryPolicyFactoryConfig.getDurationConfig();

        assertNotNull(expiryPolicyFactoryConfig);
        assertNotNull(timedExpiryPolicyFactoryConfig);
        assertNotNull(durationConfig);
        assertNull(expiryPolicyFactoryConfig.getClassName());

        assertEquals(ExpiryPolicyType.TOUCHED, timedExpiryPolicyFactoryConfig.getExpiryPolicyType());
        assertEquals(4, durationConfig.getDurationAmount());
        assertEquals(TimeUnit.SECONDS, durationConfig.getTimeUnit());
    }

    @Test
    public void cacheConfigXmlTest_TimedEternalTouchedPolicyFactory() {
        Config config = instance1.getConfig();

        CacheSimpleConfig cacheWithTimedEternalExpiryPolicyFactoryConfig =
                config.getCacheConfig("cacheWithTimedEternalExpiryPolicyFactory");
        ExpiryPolicyFactoryConfig expiryPolicyFactoryConfig =
                cacheWithTimedEternalExpiryPolicyFactoryConfig.getExpiryPolicyFactoryConfig();
        TimedExpiryPolicyFactoryConfig timedExpiryPolicyFactoryConfig =
                expiryPolicyFactoryConfig.getTimedExpiryPolicyFactoryConfig();
        DurationConfig durationConfig = timedExpiryPolicyFactoryConfig.getDurationConfig();

        assertNotNull(expiryPolicyFactoryConfig);
        assertNotNull(timedExpiryPolicyFactoryConfig);
        assertNull(durationConfig);
        assertNull(expiryPolicyFactoryConfig.getClassName());

        assertEquals(ExpiryPolicyType.ETERNAL, timedExpiryPolicyFactoryConfig.getExpiryPolicyType());
    }

    @Test
    public void cacheConfigXmlTest_PartitionLostListener() {
        Config config = instance1.getConfig();

        CacheSimpleConfig cacheWithPartitionLostListenerConfig =
                config.getCacheConfig("cacheWithPartitionLostListener");
        List<CachePartitionLostListenerConfig> partitionLostListenerConfigs =
                cacheWithPartitionLostListenerConfig.getPartitionLostListenerConfigs();

        assertNotNull(partitionLostListenerConfigs);
        assertEquals(1, partitionLostListenerConfigs.size());
        assertEquals(partitionLostListenerConfigs.get(0).getClassName(), "DummyCachePartitionLostListenerImpl");
        assertNotNull(partitionLostListenerConfigs);
        assertEquals(1, partitionLostListenerConfigs.size());
        assertEquals(partitionLostListenerConfigs.get(0).getClassName(), "DummyCachePartitionLostListenerImpl");
    }

    @Test
    public void cacheConfigXmlTest_ClusterSplitBrainProtection() {
        assertNotNull(instance1);

        CacheSimpleConfig simpleConfig = instance1.getConfig().getCacheConfig("cacheWithSplitBrainProtectionRef");

        assertNotNull(simpleConfig);

        assertEquals("cacheSplitBrainProtectionRefString", simpleConfig.getSplitBrainProtectionName());
    }

    @Test
    public void cacheConfigXmlTest_DefaultMergePolicy() {
        assertNotNull(instance1);

        CacheSimpleConfig cacheWithDefaultMergePolicyConfig =
                instance1.getConfig().getCacheConfig("cacheWithDefaultMergePolicy");

        assertNotNull(cacheWithDefaultMergePolicyConfig);
        assertEquals(MergePolicyConfig.DEFAULT_MERGE_POLICY,
                cacheWithDefaultMergePolicyConfig.getMergePolicyConfig().getPolicy());
    }

    @Test
    public void cacheConfigXmlTest_CustomMergePolicy() {
        assertNotNull(instance1);

        CacheSimpleConfig cacheWithCustomMergePolicyConfig =
                instance1.getConfig().getCacheConfig("cacheWithCustomMergePolicy");

        assertNotNull(cacheWithCustomMergePolicyConfig);
        assertEquals("MyDummyMergePolicy", cacheWithCustomMergePolicyConfig.getMergePolicyConfig().getPolicy());
    }

    @Test
    public void cacheConfigXmlTest_ComparatorClassName() {
        assertNotNull(instance1);

        CacheSimpleConfig cacheConfigWithComparatorClassName =
                instance1.getConfig().getCacheConfig("cacheWithComparatorClassName");
        assertNotNull(cacheConfigWithComparatorClassName);

        EvictionConfig evictionConfig = cacheConfigWithComparatorClassName.getEvictionConfig();
        assertNotNull(evictionConfig);

        assertEquals("com.mycompany.MyEvictionPolicyComparator", evictionConfig.getComparatorClassName());
    }

    @Test
    public void cacheConfigXmlTest_ComparatorBean() {
        assertNotNull(instance1);

        CacheSimpleConfig cacheConfigWithComparatorClassName =
                instance1.getConfig().getCacheConfig("cacheWithComparatorBean");
        assertNotNull(cacheConfigWithComparatorClassName);

        EvictionConfig evictionConfig = cacheConfigWithComparatorClassName.getEvictionConfig();
        assertNotNull(evictionConfig);

        assertEquals(MyEvictionPolicyComparator.class, evictionConfig.getComparator().getClass());
    }

    @Test
    public void cacheConfigXmlTest_SimpleWriterLoader() {
        assertNotNull(instance1);

        CacheSimpleConfig config =
                instance1.getConfig().getCacheConfig("cacheWithSimpleWriterAndLoader");
        assertNotNull(config);

        assertEquals("com.hazelcast.config.CacheConfigTest$EmptyCacheWriter", config.getCacheWriter());
        assertEquals("com.hazelcast.config.CacheConfigTest$MyCacheLoader", config.getCacheLoader());
    }
}
