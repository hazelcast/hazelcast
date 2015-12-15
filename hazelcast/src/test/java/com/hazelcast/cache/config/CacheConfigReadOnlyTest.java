/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.cache.config;

import com.hazelcast.config.CacheConfig;
import com.hazelcast.config.CacheConfigReadOnly;
import com.hazelcast.config.CacheSimpleConfig;
import com.hazelcast.config.CacheSimpleConfigReadOnly;
import com.hazelcast.config.HotRestartConfig;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class CacheConfigReadOnlyTest {

    private CacheConfigReadOnly getCacheConfigReadOnly() {
        return new CacheConfig().getAsReadOnly();
    }

    @Test(expected = UnsupportedOperationException.class)
    public void addingCacheEntryListenerConfigurationOnReadOnlyCacheConfigShouldFail() {
        getCacheConfigReadOnly().addCacheEntryListenerConfiguration(null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void removingCacheEntryListenerConfigurationOnReadOnlyCacheConfigShouldFail() {
        getCacheConfigReadOnly().removeCacheEntryListenerConfiguration(null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void settingHotRestartEnabledOfReadOnlyCacheConfigShouldFail() {
        getCacheConfigReadOnly().setHotRestartConfig(new HotRestartConfig());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void settingPartitionLostListenerConfigsOnReadOnlyCacheConfigShouldFail() {
        getCacheConfigReadOnly().setPartitionLostListenerConfigs(null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void settingExpiryPolicyFactoryOfReadOnlyCacheConfigShouldFail() {
        getCacheConfigReadOnly().setExpiryPolicyFactory(null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void settingCacheLoaderFactoryOfReadOnlyCacheConfigShouldFail() {
        getCacheConfigReadOnly().setCacheLoaderFactory(null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void settingCacheWriterFactoryOfReadOnlyCacheConfigShouldFail() {
        getCacheConfigReadOnly().setCacheWriterFactory(null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void settingNameOfReadOnlyCacheConfigShouldFail() {
        getCacheConfigReadOnly().setName("my-cache");
    }

    @Test(expected = UnsupportedOperationException.class)
    public void settingBackupCountOfReadOnlyCacheConfigShouldFail() {
        getCacheConfigReadOnly().setBackupCount(1);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void settingAsyncBackupCountOfReadOnlyCacheConfigShouldFail() {
        getCacheConfigReadOnly().setAsyncBackupCount(1);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void settingReadThroughOfReadOnlyCacheConfigShouldFail() {
        getCacheConfigReadOnly().setReadThrough(true);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void settingWriteThroughOfReadOnlyCacheConfigShouldFail() {
        getCacheConfigReadOnly().setWriteThrough(true);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void settingStoreByValueOfReadOnlyCacheConfigShouldFail() {
        getCacheConfigReadOnly().setStoreByValue(true);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void settingTypesOfReadOnlyCacheConfigShouldFail() {
        getCacheConfigReadOnly().setTypes(String.class, String.class);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void settingInMemoryFormatOfReadOnlyCacheConfigShouldFail() {
        getCacheConfigReadOnly().setInMemoryFormat(InMemoryFormat.BINARY);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void settingManagementOfReadOnlyCacheConfigShouldFail() {
        getCacheConfigReadOnly().setManagementEnabled(true);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void settingStatisticsEnabledOfReadOnlyCacheConfigShouldFail() {
        getCacheConfigReadOnly().setStatisticsEnabled(true);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void settingQuorumNameOfReadOnlyCacheConfigShouldFail() {
        getCacheConfigReadOnly().setQuorumName("my-quorum");
    }

    @Test(expected = UnsupportedOperationException.class)
    public void settingMergePolicyOfReadOnlyCacheConfigShouldFail() {
        getCacheConfigReadOnly().setMergePolicy("my-merge-policy");
    }

    @Test(expected = UnsupportedOperationException.class)
    public void settingEvictionConfigOfReadOnlyCacheConfigShouldFail() {
        getCacheConfigReadOnly().setEvictionConfig(null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void settingWanReplicationRefOfReadOnlyCacheConfigShouldFail() {
        getCacheConfigReadOnly().setWanReplicationRef(null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void settingManagerPrefixOfReadOnlyCacheConfigShouldFail() {
        getCacheConfigReadOnly().setManagerPrefix("manager-prefix");
    }

    @Test(expected = UnsupportedOperationException.class)
    public void settingUriStringToReadOnlyCacheConfigShouldFail() {
        getCacheConfigReadOnly().setUriString("my-uri");
    }

    @Test(expected = UnsupportedOperationException.class)
    public void settingCacheEntryListenersOfReadOnlyCacheSimpleConfigShouldFail() {
        new CacheSimpleConfigReadOnly(new CacheSimpleConfig()).setCacheEntryListeners(null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void addingCacheEntryListenerToReadOnlyCacheSimpleConfigShouldFail() {
        new CacheSimpleConfigReadOnly(new CacheSimpleConfig()).addEntryListenerConfig(null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void settingCacheLoaderFactoryOfReadOnlyCacheSimpleConfigShouldFail() {
        new CacheSimpleConfigReadOnly(new CacheSimpleConfig()).setCacheLoaderFactory("cache-loader-factory");
    }

    @Test(expected = UnsupportedOperationException.class)
    public void settingCacheWriterFactoryOfReadOnlyCacheSimpleConfigShouldFail() {
        new CacheSimpleConfigReadOnly(new CacheSimpleConfig()).setCacheWriterFactory("cache-writer-factory");
    }

    @Test(expected = UnsupportedOperationException.class)
    public void settingExpiryPolicyFactoryOfReadOnlyCacheSimpleConfigShouldFail() {
        new CacheSimpleConfigReadOnly(new CacheSimpleConfig()).setExpiryPolicyFactory("my-expiry-policy-factory");
    }

    @Test(expected = UnsupportedOperationException.class)
    public void settingExpiryPolicyFactoryConfigOfReadOnlyCacheSimpleConfigShouldFail() {
        new CacheSimpleConfigReadOnly(new CacheSimpleConfig()).setExpiryPolicyFactoryConfig(null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void settingPartitionLostListenerConfigsOfReadOnlyCacheSimpleConfigShouldFail() {
        new CacheSimpleConfigReadOnly(new CacheSimpleConfig()).setPartitionLostListenerConfigs(null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void addingPartitionLostListenerConfigToReadOnlyCacheSimpleConfigShouldFail() {
        new CacheSimpleConfigReadOnly(new CacheSimpleConfig()).addCachePartitionLostListenerConfig(null);
    }

}
