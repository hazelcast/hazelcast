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

package com.hazelcast.query.impl.getters;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class JsonGetterContextCacheTest {

    @Test
    public void testGetReturnsTheSameObject() {
        JsonGetterContextCache cache = new JsonGetterContextCache(4, 2);
        JsonGetterContext contextA = cache.getContext("a");
        JsonGetterContext contextB = cache.getContext("b");
        JsonGetterContext contextC = cache.getContext("c");
        JsonGetterContext contextD = cache.getContext("d");

        assertSame(contextA, cache.getContext("a"));
        assertSame(contextB, cache.getContext("b"));
        assertSame(contextC, cache.getContext("c"));
        assertSame(contextD, cache.getContext("d"));
    }

    @Test
    public void testCacheSizeIsLimited() {
        JsonGetterContextCache cache = new JsonGetterContextCache(3, 2);
        cache.getContext("a");
        cache.getContext("b");
        cache.getContext("c");
        cache.getContext("d");
        cache.getContext("e");
        cache.getContext("f");
        cache.getContext("g");
        cache.getContext("h");
        cache.getContext("i");

        assertTrue(cache.getCacheSize() <= 3);
    }

    @Test
    public void testMostRecentlyAddedElementIsNotImmediatelyEvicted() {
        JsonGetterContextCache cache = new JsonGetterContextCache(3, 2);
        cache.getContext("a");
        cache.getContext("b");
        cache.getContext("c");
        JsonGetterContext contextD = cache.getContext("d");

        assertSame(contextD, cache.getContext("d"));
    }
}
