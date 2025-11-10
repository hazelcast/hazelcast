/*
 * Copyright (c) 2008-2025, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.impl.proxy.MapProxyImpl;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.TimeUnit;

/**
 * Member implementation for basic map methods nullability tests
 */
@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class MemberMapNullTest extends AbstractMapNullTest {

    private HazelcastInstance instance;

    @Before
    public void setup() {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory();
        instance = factory.newHazelcastInstance();
    }

    @Override
    protected boolean isNotClient() {
        return true;
    }

    @Override
    protected HazelcastInstance getDriver() {
        return instance;
    }

    @Override
    @Test
    public void testNullability() {
        super.testNullability();
    }

    @Override
    @SuppressWarnings({"unchecked", "rawtypes"})
    protected void testNullability(Object key, Object value) {
        super.testNullability(key, value);

        assertThrowsNPE(m -> ((MapProxyImpl) m).putIfAbsentAsync(key, ""));
        assertThrowsNPE(m -> ((MapProxyImpl) m).putIfAbsentAsync("", value));
        assertThrowsNPE(m -> ((MapProxyImpl) m).putIfAbsentAsync(key, "", -1, TimeUnit.SECONDS));
        assertThrowsNPE(m -> ((MapProxyImpl) m).putIfAbsentAsync("", value, -1, TimeUnit.SECONDS));
        assertThrowsNPE(m -> ((MapProxyImpl) m).putIfAbsentAsync("", "", -1, null));
        assertThrowsNPE(m -> ((MapProxyImpl) m).putIfAbsentAsync(key, "", -1, TimeUnit.SECONDS, -1, TimeUnit.SECONDS));
        assertThrowsNPE(m -> ((MapProxyImpl) m).putIfAbsentAsync("", value, -1, TimeUnit.SECONDS, -1, TimeUnit.SECONDS));
        assertThrowsNPE(m -> ((MapProxyImpl) m).putIfAbsentAsync("", "", -1, null, -1, TimeUnit.SECONDS));
        assertThrowsNPE(m -> ((MapProxyImpl) m).putIfAbsentAsync("", "", -1, TimeUnit.SECONDS, -1, null));
    }

    @Test
    @Ignore("Needs additional validation in member proxy - HZG-446")
    @Override
    public void testNullabilityNullHeapData() {
        super.testNullabilityNullHeapData();
    }

    @Test
    @Ignore("Needs additional validation in member proxy - HZG-446")
    @Override
    public void testNullabilityEmptyHeapData() {
        super.testNullabilityEmptyHeapData();
    }
}
