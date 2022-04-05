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

package com.hazelcast.internal.util.collection;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ComposedKeyMapTest {

    private ComposedKeyMap<String, String, String> map = new ComposedKeyMap<String, String, String>();

    @Test
    public void givenEmpty_whenPut_thenReturnNull() {
        String prevValue = map.put("1", "2", "value");
        assertNull(prevValue);
    }

    @Test
    public void givenValueAssociated_whenPut_thenReturnPreviousValue() {
        map.put("1", "2", "prevValue");
        String prevValue = map.put("1", "2", "newValue");
        assertEquals("prevValue", prevValue);
    }

    @Test
    public void givenEmpty_whenGet_thenReturnNull() {
        String value = map.get("key1", "key2");
        assertNull(value);
    }

    @Test
    public void givenHasEntry_whenGetWithDifferent2ndKey_thenReturnNull() {
        map.put("key1", "key2", "value");

        String value = map.get("key1", "wrongKey2");
        assertNull(value);
    }

    @Test
    public void givenHasEntry_whenGetWithTheSameKeys_thenReturnValue() {
        map.put("key1", "key2", "value");

        String value = map.get("key1", "key2");
        assertEquals("value", value);
    }
}
