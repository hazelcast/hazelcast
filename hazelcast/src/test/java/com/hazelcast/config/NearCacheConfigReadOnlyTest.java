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

import com.hazelcast.internal.config.NearCacheConfigReadOnly;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class NearCacheConfigReadOnlyTest {

    private NearCacheConfig getReadOnlyConfig() {
        return new NearCacheConfigReadOnly(new NearCacheConfig());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void setNameOnReadOnlyNearCacheConfigShouldFail() {
        getReadOnlyConfig().setName("anyName");
    }

    @Test(expected = UnsupportedOperationException.class)
    public void setTimeToLiveSecondsOnReadOnlyNearCacheConfigShouldFail() {
        getReadOnlyConfig().setTimeToLiveSeconds(1512);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void setMaxIdleSecondsOnReadOnlyNearCacheConfigShouldFail() {
        getReadOnlyConfig().setMaxIdleSeconds(523);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void setSerializeKeysOnReadOnlyNearCacheConfigShouldFail() {
        getReadOnlyConfig().setSerializeKeys(true);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void setInvalidateOnChangeOnReadOnlyNearCacheConfigShouldFail() {
        getReadOnlyConfig().setInvalidateOnChange(true);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void setInMemoryFormatOnReadOnlyNearCacheConfigShouldFail() {
        getReadOnlyConfig().setInMemoryFormat(InMemoryFormat.OBJECT);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void setInMemoryFormatAsStringOnReadOnlyNearCacheConfigShouldFail() {
        getReadOnlyConfig().setInMemoryFormat(InMemoryFormat.OBJECT.name());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void setCacheLocalEntriesOnReadOnlyNearCacheConfigShouldFail() {
        getReadOnlyConfig().setCacheLocalEntries(true);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void setLocalUpdatePolicyOnReadOnlyNearCacheConfigShouldFail() {
        getReadOnlyConfig().setLocalUpdatePolicy(NearCacheConfig.LocalUpdatePolicy.CACHE_ON_UPDATE);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void setEvictionConfigOnReadOnlyNearCacheConfigShouldFail() {
        getReadOnlyConfig().setEvictionConfig(new EvictionConfig());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void getEvictionConfigOnReadOnlyNearCacheConfigShouldReturnReadOnly() {
        getReadOnlyConfig().getEvictionConfig().setSize(500);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void setPreloaderConfigOnReadOnlyNearCacheConfigShouldFail() {
        getReadOnlyConfig().setPreloaderConfig(new NearCachePreloaderConfig());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void getPreloaderConfigOnReadOnlyNearCacheConfigShouldReturnReadOnly() {
        getReadOnlyConfig().getPreloaderConfig().setEnabled(true);
    }
}
