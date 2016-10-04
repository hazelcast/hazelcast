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

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class NearCacheConfigReadOnlyTest {

    private NearCacheConfigReadOnly getNearCacheConfigReadOnly() {
        return new NearCacheConfig().getAsReadOnly();
    }

    @Test(expected = UnsupportedOperationException.class)
    public void setNameOnReadOnlyCacheConfigShouldFail() {
        getNearCacheConfigReadOnly().setName("anyName");
    }

    @Test(expected = UnsupportedOperationException.class)
    public void setTimeToLiveSecondsOnReadOnlyCacheConfigShouldFail() {
        getNearCacheConfigReadOnly().setTimeToLiveSeconds(1512);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void setMaxSizeOnReadOnlyCacheConfigShouldFail() {
        getNearCacheConfigReadOnly().setMaxSize(125124);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void setEvictionPolicyOnReadOnlyCacheConfigShouldFail() {
        getNearCacheConfigReadOnly().setEvictionPolicy(EvictionPolicy.NONE.name());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void setMaxIdleSecondsOnReadOnlyCacheConfigShouldFail() {
        getNearCacheConfigReadOnly().setMaxIdleSeconds(523);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void setInvalidateOnChangeOnReadOnlyCacheConfigShouldFail() {
        getNearCacheConfigReadOnly().setInvalidateOnChange(true);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void setInMemoryFormatOnReadOnlyCacheConfigShouldFail() {
        getNearCacheConfigReadOnly().setInMemoryFormat(InMemoryFormat.OBJECT);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void setInMemoryFormatAsStringOnReadOnlyCacheConfigShouldFail() {
        getNearCacheConfigReadOnly().setInMemoryFormat(InMemoryFormat.OBJECT.name());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void setCacheLocalEntriesOnReadOnlyCacheConfigShouldFail() {
        getNearCacheConfigReadOnly().setCacheLocalEntries(true);
    }
}
