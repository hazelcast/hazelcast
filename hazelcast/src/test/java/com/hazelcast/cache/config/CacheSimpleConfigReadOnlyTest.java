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

package com.hazelcast.cache.config;

import com.hazelcast.config.CacheSimpleConfig;
import com.hazelcast.config.CacheSimpleConfigReadOnly;
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
        new CacheSimpleConfigReadOnly(new CacheSimpleConfig()).setName("my-cache");
    }

    @Test(expected = UnsupportedOperationException.class)
    public void settingBackupCountOfReadOnlyCacheSimpleConfigShouldFail() {
        new CacheSimpleConfigReadOnly(new CacheSimpleConfig()).setBackupCount(1);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void settingAsyncBackupCountOfReadOnlyCacheSimpleConfigShouldFail() {
        new CacheSimpleConfigReadOnly(new CacheSimpleConfig()).setAsyncBackupCount(1);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void settingReadThroughOfReadOnlyCacheSimpleConfigShouldFail() {
        new CacheSimpleConfigReadOnly(new CacheSimpleConfig()).setReadThrough(true);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void settingWriteThroughOfReadOnlyCacheSimpleConfigShouldFail() {
        new CacheSimpleConfigReadOnly(new CacheSimpleConfig()).setWriteThrough(true);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void settingKeyTypeOfReadOnlyCacheSimpleConfigShouldFail() {
        new CacheSimpleConfigReadOnly(new CacheSimpleConfig()).setKeyType("key-type");
    }

    @Test(expected = UnsupportedOperationException.class)
    public void settingValueTypeOfReadOnlyCacheSimpleConfigShouldFail() {
        new CacheSimpleConfigReadOnly(new CacheSimpleConfig()).setValueType("value-type");
    }

    @Test(expected = UnsupportedOperationException.class)
    public void settingInMemoryFormatOfReadOnlyCacheSimpleConfigShouldFail() {
        new CacheSimpleConfigReadOnly(new CacheSimpleConfig()).setInMemoryFormat(InMemoryFormat.BINARY);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void settingManagementOfReadOnlyCacheSimpleConfigShouldFail() {
        new CacheSimpleConfigReadOnly(new CacheSimpleConfig()).setManagementEnabled(true);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void settingStatisticsEnabledOfReadOnlyCacheSimpleConfigShouldFail() {
        new CacheSimpleConfigReadOnly(new CacheSimpleConfig()).setStatisticsEnabled(true);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void settingQuorumNameOfReadOnlyCacheSimpleConfigShouldFail() {
        new CacheSimpleConfigReadOnly(new CacheSimpleConfig()).setQuorumName("my-quorum");
    }

    @Test(expected = UnsupportedOperationException.class)
    public void settingMergePolicyOfReadOnlyCacheSimpleConfigShouldFail() {
        new CacheSimpleConfigReadOnly(new CacheSimpleConfig()).setMergePolicy("my-merge-policy");
    }

    @Test(expected = UnsupportedOperationException.class)
    public void settingEvictionConfigOfReadOnlyCacheSimpleConfigShouldFail() {
        new CacheSimpleConfigReadOnly(new CacheSimpleConfig()).setEvictionConfig(null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void settingWanReplicationRefOfReadOnlyCacheSimpleConfigShouldFail() {
        new CacheSimpleConfigReadOnly(new CacheSimpleConfig()).setWanReplicationRef(null);
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

    @Test(expected = UnsupportedOperationException.class)
    public void settingDisablePerEntryInvalidationEventsOfReadOnlyCacheSimpleConfigShouldFail() {
        new CacheSimpleConfigReadOnly(new CacheSimpleConfig()).setDisablePerEntryInvalidationEvents(true);
    }

}
