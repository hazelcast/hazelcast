/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.config.CacheSimpleConfig;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class CacheSimpleConfigReadOnlyTest {

    @Test(expected = UnsupportedOperationException.class)
    public void settingNameOfReadOnlyCacheSimpleConfigShouldFail() {
        new CacheSimpleConfig.CacheSimpleConfigReadOnly(new CacheSimpleConfig()).setName("my-cache");
    }

    @Test(expected = UnsupportedOperationException.class)
    public void settingBackupCountOfReadOnlyCacheSimpleConfigShouldFail() {
        new CacheSimpleConfig.CacheSimpleConfigReadOnly(new CacheSimpleConfig()).setBackupCount(1);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void settingAsyncBackupCountOfReadOnlyCacheSimpleConfigShouldFail() {
        new CacheSimpleConfig.CacheSimpleConfigReadOnly(new CacheSimpleConfig()).setAsyncBackupCount(1);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void settingReadThroughOfReadOnlyCacheSimpleConfigShouldFail() {
        new CacheSimpleConfig.CacheSimpleConfigReadOnly(new CacheSimpleConfig()).setReadThrough(true);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void settingWriteThroughOfReadOnlyCacheSimpleConfigShouldFail() {
        new CacheSimpleConfig.CacheSimpleConfigReadOnly(new CacheSimpleConfig()).setWriteThrough(true);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void settingKeyTypeOfReadOnlyCacheSimpleConfigShouldFail() {
        new CacheSimpleConfig.CacheSimpleConfigReadOnly(new CacheSimpleConfig()).setKeyType("key-type");
    }

    @Test(expected = UnsupportedOperationException.class)
    public void settingValueTypeOfReadOnlyCacheSimpleConfigShouldFail() {
        new CacheSimpleConfig.CacheSimpleConfigReadOnly(new CacheSimpleConfig()).setValueType("value-type");
    }

    @Test(expected = UnsupportedOperationException.class)
    public void settingInMemoryFormatOfReadOnlyCacheSimpleConfigShouldFail() {
        new CacheSimpleConfig.CacheSimpleConfigReadOnly(new CacheSimpleConfig()).setInMemoryFormat(InMemoryFormat.BINARY);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void settingManagementOfReadOnlyCacheSimpleConfigShouldFail() {
        new CacheSimpleConfig.CacheSimpleConfigReadOnly(new CacheSimpleConfig()).setManagementEnabled(true);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void settingStatisticsEnabledOfReadOnlyCacheSimpleConfigShouldFail() {
        new CacheSimpleConfig.CacheSimpleConfigReadOnly(new CacheSimpleConfig()).setStatisticsEnabled(true);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void settingQuorumNameOfReadOnlyCacheSimpleConfigShouldFail() {
        new CacheSimpleConfig.CacheSimpleConfigReadOnly(new CacheSimpleConfig()).setQuorumName("my-quorum");
    }

    @Test(expected = UnsupportedOperationException.class)
    public void settingMergePolicyOfReadOnlyCacheSimpleConfigShouldFail() {
        new CacheSimpleConfig.CacheSimpleConfigReadOnly(new CacheSimpleConfig()).setMergePolicy("my-merge-policy");
    }

    @Test(expected = UnsupportedOperationException.class)
    public void settingEvictionConfigOfReadOnlyCacheSimpleConfigShouldFail() {
        new CacheSimpleConfig.CacheSimpleConfigReadOnly(new CacheSimpleConfig()).setEvictionConfig(null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void settingWanReplicationRefOfReadOnlyCacheSimpleConfigShouldFail() {
        new CacheSimpleConfig.CacheSimpleConfigReadOnly(new CacheSimpleConfig()).setWanReplicationRef(null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void settingCacheEntryListenersOfReadOnlyCacheSimpleConfigShouldFail() {
        new CacheSimpleConfig.CacheSimpleConfigReadOnly(new CacheSimpleConfig()).setCacheEntryListeners(null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void addingCacheEntryListenerToReadOnlyCacheSimpleConfigShouldFail() {
        new CacheSimpleConfig.CacheSimpleConfigReadOnly(new CacheSimpleConfig()).addEntryListenerConfig(null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void settingCacheLoaderFactoryOfReadOnlyCacheSimpleConfigShouldFail() {
        new CacheSimpleConfig.CacheSimpleConfigReadOnly(new CacheSimpleConfig()).setCacheLoaderFactory("cache-loader-factory");
    }

    @Test(expected = UnsupportedOperationException.class)
    public void settingCacheWriterFactoryOfReadOnlyCacheSimpleConfigShouldFail() {
        new CacheSimpleConfig.CacheSimpleConfigReadOnly(new CacheSimpleConfig()).setCacheWriterFactory("cache-writer-factory");
    }

    @Test(expected = UnsupportedOperationException.class)
    public void settingExpiryPolicyFactoryOfReadOnlyCacheSimpleConfigShouldFail() {
        new CacheSimpleConfig.CacheSimpleConfigReadOnly(new CacheSimpleConfig()).setExpiryPolicyFactory("my-expiry-policy-factory");
    }

    @Test(expected = UnsupportedOperationException.class)
    public void settingExpiryPolicyFactoryConfigOfReadOnlyCacheSimpleConfigShouldFail() {
        new CacheSimpleConfig.CacheSimpleConfigReadOnly(new CacheSimpleConfig()).setExpiryPolicyFactoryConfig(null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void settingPartitionLostListenerConfigsOfReadOnlyCacheSimpleConfigShouldFail() {
        new CacheSimpleConfig.CacheSimpleConfigReadOnly(new CacheSimpleConfig()).setPartitionLostListenerConfigs(null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void addingPartitionLostListenerConfigToReadOnlyCacheSimpleConfigShouldFail() {
        new CacheSimpleConfig.CacheSimpleConfigReadOnly(new CacheSimpleConfig()).addCachePartitionLostListenerConfig(null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void settingDisablePerEntryInvalidationEventsOfReadOnlyCacheSimpleConfigShouldFail() {
        new CacheSimpleConfig.CacheSimpleConfigReadOnly(new CacheSimpleConfig()).setDisablePerEntryInvalidationEvents(true);
    }

}
