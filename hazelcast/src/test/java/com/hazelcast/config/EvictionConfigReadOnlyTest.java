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

import com.hazelcast.internal.config.EvictionConfigReadOnly;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class EvictionConfigReadOnlyTest {

    private EvictionConfig getReadOnlyConfig() {
        return new EvictionConfigReadOnly(new EvictionConfig());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void setSizeOnReadOnlyEvictionConfigShouldFail() {
        getReadOnlyConfig().setSize(100);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void setMaxSizePolicyOnReadOnlyEvictionConfigShouldFail() {
        getReadOnlyConfig().setMaxSizePolicy(null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void setEvictionPolicyOnReadOnlyEvictionConfigShouldFail() {
        getReadOnlyConfig().setEvictionPolicy(null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void setComparatorClassNameOnReadOnlyEvictionConfigShouldFail() {
        getReadOnlyConfig().setComparatorClassName("myComparatorClassName");
    }

    @Test(expected = UnsupportedOperationException.class)
    public void setComparatorOnReadOnlyEvictionConfigShouldFail() {
        getReadOnlyConfig().setComparator(null);
    }

    @Test
    public void copy_constructor_copies_right_value_of_field_sizeConfigured() {
        EvictionConfig evictionConfig = new EvictionConfig();
        EvictionConfig asReadOnly = new EvictionConfigReadOnly(evictionConfig);

        assertEquals(evictionConfig.sizeConfigured, asReadOnly.sizeConfigured);
    }
}
