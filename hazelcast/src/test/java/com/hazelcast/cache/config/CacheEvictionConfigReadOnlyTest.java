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

import com.hazelcast.config.CacheEvictionConfig;
import com.hazelcast.config.EvictionConfig;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class CacheEvictionConfigReadOnlyTest {

    private EvictionConfig getEvictionConfigReadOnly() {
        return new CacheEvictionConfig().getAsReadOnly();
    }

    @Test(expected = UnsupportedOperationException.class)
    public void settingSizeOnReadOnlyCacheConfigShouldFail() {
        getEvictionConfigReadOnly().setSize(100);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void settingMaxSizePolicyOnReadOnlyCacheConfigShouldFail() {
        getEvictionConfigReadOnly().setMaximumSizePolicy(null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void settingEvictionPolicyOnReadOnlyCacheConfigShouldFail() {
        getEvictionConfigReadOnly().setEvictionPolicy(null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void settingComparatorClassNameOnReadOnlyCacheConfigShouldFail() {
        getEvictionConfigReadOnly().setComparatorClassName("mycomparator");
    }

    @Test(expected = UnsupportedOperationException.class)
    public void settingComparatorOnReadOnlyCacheConfigShouldFail() {
        getEvictionConfigReadOnly().setComparator(null);
    }

}
