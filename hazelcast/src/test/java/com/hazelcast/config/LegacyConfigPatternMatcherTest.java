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

package com.hazelcast.config;

import com.hazelcast.config.matcher.LegacyWildcardConfigPatternMatcher;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class LegacyConfigPatternMatcherTest {

    @Test
    public void testMapConfigWildcardStartsWith() {
        MapConfig mapConfig = new MapConfig().setName("bc*");

        Config config = new Config();
        config.setConfigPatternMatcher(new LegacyWildcardConfigPatternMatcher());
        config.addMapConfig(mapConfig);

        // we should match this
        assertEquals(mapConfig, config.getMapConfig("bc.xyz"));
        assertEquals(mapConfig, config.getMapConfig("bc.xyz@foo"));

        // we should also match this (contains)
        assertEquals(mapConfig, config.getMapConfig("abc.xyz"));
        assertEquals(mapConfig, config.getMapConfig("abc.xyz@foo"));
    }

    @Test
    public void testMapConfigWildcardEndsWith() {
        MapConfig mapConfig = new MapConfig().setName("*ab");

        Config config = new Config();
        config.setConfigPatternMatcher(new LegacyWildcardConfigPatternMatcher());
        config.addMapConfig(mapConfig);

        // we should match this
        assertEquals(mapConfig, config.getMapConfig("xyz.ab"));
        assertEquals(mapConfig, config.getMapConfig("xyz.ab@foo"));

        // we should also match this (contains)
        assertEquals(mapConfig, config.getMapConfig("xyz.abc"));
        assertEquals(mapConfig, config.getMapConfig("xyz.abc@foo"));
    }
}
