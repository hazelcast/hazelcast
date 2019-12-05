/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.util;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class MapUtilTest {

    @Test
    public void isConstructorPrivate() {
        HazelcastTestSupport.assertUtilityConstructor(MapUtil.class);
    }

    @Test
    public void isNullOrEmpty_whenNull() {
        assertTrue(MapUtil.isNullOrEmpty(null));
    }

    @Test
    public void isNullOrEmpty_whenEmpty() {
        assertTrue(MapUtil.isNullOrEmpty(new HashMap()));
    }

    @Test
    public void isNullOrEmpty_whenNotEmpty() {
        Map<String, String> map = new HashMap();
        map.put("a", "b");
        assertFalse(MapUtil.isNullOrEmpty(map));
    }

}
