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

package com.hazelcast.config.helpers;

import com.hazelcast.config.CacheConfig;
import com.hazelcast.config.CacheConfigTest;
import com.hazelcast.config.EvictionConfig;
import com.hazelcast.config.HotRestartConfig;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.WanReplicationRef;
import com.hazelcast.internal.dynamicconfig.DynamicConfigTest;

import javax.cache.configuration.MutableCacheEntryListenerConfiguration;
import java.util.Collections;

import static com.hazelcast.config.MaxSizePolicy.ENTRY_COUNT;
import static com.hazelcast.config.EvictionPolicy.RANDOM;
import static com.hazelcast.test.HazelcastTestSupport.randomName;

public abstract class CacheConfigHelper {

    public static EvictionConfig getEvictionConfigByPolicy() {
        return new EvictionConfig()
                .setSize(39)
                .setMaxSizePolicy(ENTRY_COUNT)
                .setEvictionPolicy(RANDOM);
    }

    public static EvictionConfig getEvictionConfigByClassName() {
        return new EvictionConfig()
                .setSize(39)
                .setMaxSizePolicy(ENTRY_COUNT)
                .setComparatorClassName("com.hazelcast.Comparator");
    }

    public static EvictionConfig getEvictionConfigByImplementation() {
        return new EvictionConfig()
                .setSize(39)
                .setMaxSizePolicy(ENTRY_COUNT)
                .setComparator(new DynamicConfigTest.SampleEvictionPolicyComparator());
    }

    public static CacheConfig newDefaultCacheConfig(String name) {
        return new CacheConfig(name);
    }

    public static CacheConfig newCompleteCacheConfig(String name) {
        CacheConfig cacheConfig = new CacheConfig();
        cacheConfig.setName(name);
        cacheConfig.setSplitBrainProtectionName("split-brain-protection");
        cacheConfig.setInMemoryFormat(InMemoryFormat.OBJECT);
        cacheConfig.setBackupCount(3);
        cacheConfig.setAsyncBackupCount(2);
        cacheConfig.setWanReplicationRef(new WanReplicationRef(randomName(),
                "com.hazelcast.MergePolicy", Collections.singletonList("filter"), true));
        cacheConfig.addCacheEntryListenerConfiguration(new MutableCacheEntryListenerConfiguration(
                new CacheConfigTest.EntryListenerFactory(), null, false, true));
        cacheConfig.getMergePolicyConfig().setPolicy("mergePolicy");
        cacheConfig.setStatisticsEnabled(true);
        cacheConfig.setManagementEnabled(true);
        cacheConfig.setDisablePerEntryInvalidationEvents(true);
        cacheConfig.setKeyClassName("java.lang.Integer");
        cacheConfig.setValueClassName("java.lang.String");
        cacheConfig.setReadThrough(true);
        cacheConfig.setWriteThrough(true);
        cacheConfig.setHotRestartConfig(new HotRestartConfig().setEnabled(true).setFsync(true));
        return cacheConfig;
    }
}
