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

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class CacheDeserializedValuesTest extends HazelcastTestSupport {

    @Test
    public void parseString_whenNEVER() {
        CacheDeserializedValues cacheDeserializedValues = CacheDeserializedValues.parseString("NEVER");
        assertEquals(CacheDeserializedValues.NEVER, cacheDeserializedValues);
    }

    @Test
    public void parseString_whenNever() {
        CacheDeserializedValues cacheDeserializedValues = CacheDeserializedValues.parseString("never");
        assertEquals(CacheDeserializedValues.NEVER, cacheDeserializedValues);
    }

    @Test
    public void parseString_whenINDEX_ONLY() {
        CacheDeserializedValues cacheDeserializedValues = CacheDeserializedValues.parseString("INDEX_ONLY");
        assertEquals(CacheDeserializedValues.INDEX_ONLY, cacheDeserializedValues);
    }

    @Test
    public void parseString_whenINDEX_ONLY_withDash() {
        CacheDeserializedValues cacheDeserializedValues = CacheDeserializedValues.parseString("INDEX-ONLY");
        assertEquals(CacheDeserializedValues.INDEX_ONLY, cacheDeserializedValues);
    }

    @Test
    public void parseString_whenIndex_only() {
        CacheDeserializedValues cacheDeserializedValues = CacheDeserializedValues.parseString("index_only");
        assertEquals(CacheDeserializedValues.INDEX_ONLY, cacheDeserializedValues);
    }

    @Test
    public void parseString_whenIndex_only_withDash() {
        CacheDeserializedValues cacheDeserializedValues = CacheDeserializedValues.parseString("index-only");
        assertEquals(CacheDeserializedValues.INDEX_ONLY, cacheDeserializedValues);
    }

    @Test
    public void parseString_whenALWAYS() {
        CacheDeserializedValues cacheDeserializedValues = CacheDeserializedValues.parseString("ALWAYS");
        assertEquals(CacheDeserializedValues.ALWAYS, cacheDeserializedValues);
    }

    @Test
    public void parseString_whenAlways() {
        CacheDeserializedValues cacheDeserializedValues = CacheDeserializedValues.parseString("always");
        assertEquals(CacheDeserializedValues.ALWAYS, cacheDeserializedValues);
    }

    @Test(expected = IllegalArgumentException.class)
    public void parseString_whenUnknownString() {
        CacheDeserializedValues cacheDeserializedValues = CacheDeserializedValues.parseString("does no exist");
    }
}
