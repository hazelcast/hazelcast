/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.config;

import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.config.matcher.LegacyMatchingPointConfigPatternMatcher;
import com.hazelcast.config.matcher.MatchingPointConfigPatternMatcher;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class LegacyMatchingPointConfigPatternMatcherTest {

    @Test
    public void testNearCacheConfigWildcardMatchingPointStartsWith() {
        NearCacheConfig nearCacheConfig1 = new NearCacheConfig().setName("hazelcast.*");
        NearCacheConfig nearCacheConfig2 = new NearCacheConfig().setName("hazelcast.test.*");
        NearCacheConfig nearCacheConfig3 = new NearCacheConfig().setName("hazelcast.test.sub.*");

        ClientConfig config = new ClientConfig();
        config.setConfigPatternMatcher(new LegacyMatchingPointConfigPatternMatcher());
        config.addNearCacheConfig(nearCacheConfig1);
        config.addNearCacheConfig(nearCacheConfig2);
        config.addNearCacheConfig(nearCacheConfig3);

        // we should match this (contains)
        assertEquals(nearCacheConfig1, config.getNearCacheConfig("com.hazelcast.myNearCache"));
        assertEquals(nearCacheConfig2, config.getNearCacheConfig("com.hazelcast.test.myNearCache"));
        assertEquals(nearCacheConfig3, config.getNearCacheConfig("com.hazelcast.test.sub.myNearCache"));
    }

    @Test
    public void testNearCacheConfigWildcardMatchingPointEndsWith() {
        NearCacheConfig nearCacheConfig1 = new NearCacheConfig().setName("*.sub");
        NearCacheConfig nearCacheConfig2 = new NearCacheConfig().setName("*.test.sub");
        NearCacheConfig nearCacheConfig3 = new NearCacheConfig().setName("*.hazelcast.test.sub");

        ClientConfig config = new ClientConfig();
        config.setConfigPatternMatcher(new LegacyMatchingPointConfigPatternMatcher());
        config.addNearCacheConfig(nearCacheConfig1);
        config.addNearCacheConfig(nearCacheConfig2);
        config.addNearCacheConfig(nearCacheConfig3);

        // we should match this (contains)
        assertEquals(nearCacheConfig1, config.getNearCacheConfig("com.hazelFast.Fast.sub.myNearCache"));
        assertEquals(nearCacheConfig2, config.getNearCacheConfig("com.hazelFast.test.sub.myNearCache"));
        assertEquals(nearCacheConfig3, config.getNearCacheConfig("com.hazelcast.test.sub.myNearCache"));
    }
}
