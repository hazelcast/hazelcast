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

package com.hazelcast.config;

import com.hazelcast.internal.config.CacheConfigReadOnly;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class CacheConfigReadOnlyTest {

    private CacheConfig getReadOnlyConfig() {
        return new CacheConfigReadOnly(new CacheConfig());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void addCacheEntryListenerConfigurationOnReadOnlyCacheConfigShouldFail() {
        getReadOnlyConfig().addCacheEntryListenerConfiguration(null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void removingCacheEntryListenerConfigurationOnReadOnlyCacheConfigShouldFail() {
        getReadOnlyConfig().removeCacheEntryListenerConfiguration(null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void setHotRestartEnabledOfReadOnlyCacheConfigShouldFail() {
        getReadOnlyConfig().setHotRestartConfig(new HotRestartConfig());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void setPartitionLostListenerConfigsOnReadOnlyCacheConfigShouldFail() {
        getReadOnlyConfig().setPartitionLostListenerConfigs(null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void setExpiryPolicyFactoryOfReadOnlyCacheConfigShouldFail() {
        getReadOnlyConfig().setExpiryPolicyFactory(null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void setCacheLoaderFactoryOfReadOnlyCacheConfigShouldFail() {
        getReadOnlyConfig().setCacheLoaderFactory(null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void setCacheWriterFactoryOfReadOnlyCacheConfigShouldFail() {
        getReadOnlyConfig().setCacheWriterFactory(null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void setNameOfReadOnlyCacheConfigShouldFail() {
        getReadOnlyConfig().setName("my-cache");
    }

    @Test(expected = UnsupportedOperationException.class)
    public void setBackupCountOfReadOnlyCacheConfigShouldFail() {
        getReadOnlyConfig().setBackupCount(1);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void setAsyncBackupCountOfReadOnlyCacheConfigShouldFail() {
        getReadOnlyConfig().setAsyncBackupCount(1);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void setReadThroughOfReadOnlyCacheConfigShouldFail() {
        getReadOnlyConfig().setReadThrough(true);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void setWriteThroughOfReadOnlyCacheConfigShouldFail() {
        getReadOnlyConfig().setWriteThrough(true);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void setStoreByValueOfReadOnlyCacheConfigShouldFail() {
        getReadOnlyConfig().setStoreByValue(true);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void setTypesOfReadOnlyCacheConfigShouldFail() {
        getReadOnlyConfig().setTypes(String.class, String.class);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void setInMemoryFormatOfReadOnlyCacheConfigShouldFail() {
        getReadOnlyConfig().setInMemoryFormat(InMemoryFormat.BINARY);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void setManagementOfReadOnlyCacheConfigShouldFail() {
        getReadOnlyConfig().setManagementEnabled(true);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void setStatisticsEnabledOfReadOnlyCacheConfigShouldFail() {
        getReadOnlyConfig().setStatisticsEnabled(true);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void setSplitBrainProtectionNameOfReadOnlyCacheConfigShouldFail() {
        getReadOnlyConfig().setSplitBrainProtectionName("my-split-brain-protection");
    }

    @Test(expected = UnsupportedOperationException.class)
    public void setMergePolicyOfReadOnlyCacheConfigShouldFail() {
        getReadOnlyConfig().setMergePolicyConfig(new MergePolicyConfig());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void setEvictionConfigOfReadOnlyCacheConfigShouldFail() {
        getReadOnlyConfig().setEvictionConfig(null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void setWanReplicationRefOfReadOnlyCacheConfigShouldFail() {
        getReadOnlyConfig().setWanReplicationRef(null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void setManagerPrefixOfReadOnlyCacheConfigShouldFail() {
        getReadOnlyConfig().setManagerPrefix("manager-prefix");
    }

    @Test(expected = UnsupportedOperationException.class)
    public void setUriStringToReadOnlyCacheConfigShouldFail() {
        getReadOnlyConfig().setUriString("my-uri");
    }


    @Test(expected = UnsupportedOperationException.class)
    public void addCacheEntryListenerConfigToReadOnlyCacheSimpleConfigShouldFail() {
        getReadOnlyConfig().addCacheEntryListenerConfiguration(null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void removingCacheEntryListenerConfigToReadOnlyCacheSimpleConfigShouldFail() {
        getReadOnlyConfig().removeCacheEntryListenerConfiguration(null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void setCacheLoaderFactoryOfReadOnlyCacheSimpleConfigShouldFail() {
        getReadOnlyConfig().setCacheLoaderFactory(null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void setCacheWriterFactoryOfReadOnlyCacheSimpleConfigShouldFail() {
        getReadOnlyConfig().setCacheWriterFactory(null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void setExpiryPolicyFactoryOfReadOnlyCacheSimpleConfigShouldFail() {
        getReadOnlyConfig().setExpiryPolicyFactory(null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void setPartitionLostListenerConfigsOfReadOnlyCacheSimpleConfigShouldFail() {
        getReadOnlyConfig().setPartitionLostListenerConfigs(null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void setDisablePerEntryInvalidationEventsOfReadOnlyCacheSimpleConfigShouldFail() {
        getReadOnlyConfig().setDisablePerEntryInvalidationEvents(true);
    }
}
