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

import com.hazelcast.internal.config.CacheSimpleConfigReadOnly;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class CacheSimpleConfigReadOnlyTest {

    private CacheSimpleConfig getReadOnlyConfig() {
        return new CacheSimpleConfigReadOnly(new CacheSimpleConfig());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void setNameOfReadOnlyCacheSimpleConfigShouldFail() {
        getReadOnlyConfig().setName("my-cache");
    }

    @Test(expected = UnsupportedOperationException.class)
    public void setBackupCountOfReadOnlyCacheSimpleConfigShouldFail() {
        getReadOnlyConfig().setBackupCount(1);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void setAsyncBackupCountOfReadOnlyCacheSimpleConfigShouldFail() {
        getReadOnlyConfig().setAsyncBackupCount(1);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void setReadThroughOfReadOnlyCacheSimpleConfigShouldFail() {
        getReadOnlyConfig().setReadThrough(true);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void setWriteThroughOfReadOnlyCacheSimpleConfigShouldFail() {
        getReadOnlyConfig().setWriteThrough(true);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void setKeyTypeOfReadOnlyCacheSimpleConfigShouldFail() {
        getReadOnlyConfig().setKeyType("key-type");
    }

    @Test(expected = UnsupportedOperationException.class)
    public void setValueTypeOfReadOnlyCacheSimpleConfigShouldFail() {
        getReadOnlyConfig().setValueType("value-type");
    }

    @Test(expected = UnsupportedOperationException.class)
    public void setInMemoryFormatOfReadOnlyCacheSimpleConfigShouldFail() {
        getReadOnlyConfig().setInMemoryFormat(InMemoryFormat.BINARY);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void setManagementOfReadOnlyCacheSimpleConfigShouldFail() {
        getReadOnlyConfig().setManagementEnabled(true);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void setStatisticsEnabledOfReadOnlyCacheSimpleConfigShouldFail() {
        getReadOnlyConfig().setStatisticsEnabled(true);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void setSplitBrainProtectionNameOfReadOnlyCacheSimpleConfigShouldFail() {
        getReadOnlyConfig().setSplitBrainProtectionName("my-splitbrainprotection");
    }

    @Test(expected = UnsupportedOperationException.class)
    public void setMergePolicyOfReadOnlyCacheSimpleConfigShouldFail() {
        getReadOnlyConfig().setMergePolicyConfig(new MergePolicyConfig());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void setEvictionConfigOfReadOnlyCacheSimpleConfigShouldFail() {
        getReadOnlyConfig().setEvictionConfig(null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void setWanReplicationRefOfReadOnlyCacheSimpleConfigShouldFail() {
        getReadOnlyConfig().setWanReplicationRef(null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void setCacheEntryListenersOfReadOnlyCacheSimpleConfigShouldFail() {
        getReadOnlyConfig().setCacheEntryListeners(null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void addCacheEntryListenerToReadOnlyCacheSimpleConfigShouldFail() {
        getReadOnlyConfig().addEntryListenerConfig(null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void setCacheLoaderFactoryOfReadOnlyCacheSimpleConfigShouldFail() {
        getReadOnlyConfig().setCacheLoaderFactory("cache-loader-factory");
    }

    @Test(expected = UnsupportedOperationException.class)
    public void setCacheWriterFactoryOfReadOnlyCacheSimpleConfigShouldFail() {
        getReadOnlyConfig().setCacheWriterFactory("cache-writer-factory");
    }

    @Test(expected = UnsupportedOperationException.class)
    public void setExpiryPolicyFactoryOfReadOnlyCacheSimpleConfigShouldFail() {
        getReadOnlyConfig().setExpiryPolicyFactory("my-expiry-policy-factory");
    }

    @Test(expected = UnsupportedOperationException.class)
    public void setExpiryPolicyFactoryConfigOfReadOnlyCacheSimpleConfigShouldFail() {
        getReadOnlyConfig().setExpiryPolicyFactoryConfig(null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void setPartitionLostListenerConfigsOfReadOnlyCacheSimpleConfigShouldFail() {
        getReadOnlyConfig().setPartitionLostListenerConfigs(null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void addPartitionLostListenerConfigToReadOnlyCacheSimpleConfigShouldFail() {
        getReadOnlyConfig().addCachePartitionLostListenerConfig(null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void setDisablePerEntryInvalidationEventsOfReadOnlyCacheSimpleConfigShouldFail() {
        getReadOnlyConfig().setDisablePerEntryInvalidationEvents(true);
    }
}
