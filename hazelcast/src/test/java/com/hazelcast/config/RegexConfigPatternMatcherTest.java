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

import com.hazelcast.config.matcher.RegexConfigPatternMatcher;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.regex.Pattern;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class RegexConfigPatternMatcherTest {

    @Test
    public void testQueueConfigWithoutWildcard() {
        QueueConfig queueConfig = new QueueConfig().setName("^someQueue$").setMaxSize(133);

        Config config = new Config();
        config.setConfigPatternMatcher(new RegexConfigPatternMatcher());
        config.addQueueConfig(queueConfig);

        // non-matching name
        assertNotEquals(queueConfig.getMaxSize(), config.getQueueConfig("doesNotExist").getMaxSize());
        // non-matching name (starts with)
        assertNotEquals(queueConfig.getMaxSize(), config.getQueueConfig("_someQueue").getMaxSize());
        // non-matching name (ends with)
        assertNotEquals(queueConfig.getMaxSize(), config.getQueueConfig("someQueue_").getMaxSize());
        // non-matching case
        assertNotEquals(queueConfig.getMaxSize(), config.getQueueConfig("SomeQueue").getMaxSize());

        // TODO possible breaking change; now adding this as the first option makes rest of the options match
        assertEquals(queueConfig.getMaxSize(), config.getQueueConfig("someQueue").getMaxSize());
        assertEquals(queueConfig.getMaxSize(), config.getQueueConfig("someQueue@foo").getMaxSize());
    }

    @Test
    public void testQueueConfigRegexContains() {
        QueueConfig queueConfig = new QueueConfig().setName("abc").setMaxSize(133);

        Config config = new Config();
        config.setConfigPatternMatcher(new RegexConfigPatternMatcher());
        config.addQueueConfig(queueConfig);

        assertEquals(queueConfig.getMaxSize(), config.getQueueConfig("abcD").getMaxSize());
        assertNotEquals(queueConfig.getMaxSize(), config.getQueueConfig("abDD").getMaxSize());
    }

    @Test
    public void testQueueConfigRegexStartsWith() {
        QueueConfig queueConfig = new QueueConfig().setName("^abc").setMaxSize(133);

        Config config = new Config();
        config.setConfigPatternMatcher(new RegexConfigPatternMatcher());
        config.addQueueConfig(queueConfig);

        assertEquals(queueConfig.getMaxSize(), config.getQueueConfig("abcDe").getMaxSize());
        assertNotEquals(queueConfig.getMaxSize(), config.getQueueConfig("bcDe").getMaxSize());
    }

    @Test
    public void testMapConfigWithoutWildcard() {
        MapConfig mapConfig = new MapConfig().setName("^someMap$").setBackupCount(4);

        Config config = new Config();
        config.setConfigPatternMatcher(new RegexConfigPatternMatcher());
        config.addMapConfig(mapConfig);

        // non-matching name
        assertNotEquals(mapConfig.getBackupCount(), config.getMapConfig("doesNotExist").getBackupCount());
        // non-matching name (starts with)
        assertNotEquals(mapConfig.getBackupCount(), config.getMapConfig("_someMap").getBackupCount());
        // non-matching name (ends with)
        assertNotEquals(mapConfig.getBackupCount(), config.getMapConfig("someMap_").getBackupCount());
        // non-matching case
        assertNotEquals(mapConfig.getBackupCount(), config.getMapConfig("SomeMap").getBackupCount());

        assertEquals(mapConfig.getBackupCount(), config.getMapConfig("someMap").getBackupCount());
        assertEquals(mapConfig.getBackupCount(), config.getMapConfig("someMap@foo").getBackupCount());
    }

    @Test
    public void testMapConfigCaseInsensitive() {
        MapConfig mapConfig = new MapConfig().setName("^someMap$").setBackupCount(5);

        Config config = new Config();
        config.setConfigPatternMatcher(new RegexConfigPatternMatcher(Pattern.CASE_INSENSITIVE));
        config.addMapConfig(mapConfig);

        // non-matching name (starts with)
        assertNotEquals(mapConfig.getBackupCount(), config.getMapConfig("_SomeMap").getBackupCount());
        // non-matching name (ends with)
        assertNotEquals(mapConfig.getBackupCount(), config.getMapConfig("SomeMap_").getBackupCount());

        // case insensitive matching
        assertEquals(mapConfig.getBackupCount(), config.getMapConfig("SomeMap").getBackupCount());
    }

    @Test
    public void testMapConfigContains() {
        MapConfig mapConfig = new MapConfig().setName("bc").setBackupCount(5);

        Config config = new Config();
        config.setConfigPatternMatcher(new RegexConfigPatternMatcher());
        config.addMapConfig(mapConfig);

        // we should match this
        assertEquals(mapConfig.getBackupCount(), config.getMapConfig("bc.xyz").getBackupCount());
        assertEquals(mapConfig.getBackupCount(), config.getMapConfig("bc.xyz@foo").getBackupCount());

        // we should also match this (contains)
        assertEquals(mapConfig.getBackupCount(), config.getMapConfig("abc.xyz").getBackupCount());
        assertEquals(mapConfig.getBackupCount(), config.getMapConfig("abc.xyz@foo").getBackupCount());
    }

    @Test
    public void testMapConfigStartsWith() {
        MapConfig mapConfig = new MapConfig().setName("^abc");

        Config config = new Config();
        config.setConfigPatternMatcher(new RegexConfigPatternMatcher());
        config.addMapConfig(mapConfig);

        // we should match this
        assertEquals(mapConfig, config.getMapConfig("abc"));
        assertEquals(mapConfig, config.getMapConfig("abc@foo"));
        assertEquals(mapConfig, config.getMapConfig("abc.xyz"));
        assertEquals(mapConfig, config.getMapConfig("abc.xyz@foo"));

        // we should not match this (starts-with)
        assertNotEquals(mapConfig, config.getMapConfig("bc"));
        assertNotEquals(mapConfig, config.getMapConfig("bc@foo"));
        assertNotEquals(mapConfig, config.getMapConfig("bc.xyz"));
        assertNotEquals(mapConfig, config.getMapConfig("bc.xyz@foo"));
    }

    @Test
    public void testMapConfigEndsWith() {
        MapConfig mapConfig = new MapConfig().setName("bc$");

        Config config = new Config();
        config.setConfigPatternMatcher(new RegexConfigPatternMatcher());
        config.addMapConfig(mapConfig);

        // we should match this
        assertEquals(mapConfig, config.getMapConfig("abc"));
        assertEquals(mapConfig, config.getMapConfig("abc@foo"));
        assertEquals(mapConfig, config.getMapConfig("xyz.abc"));
        assertEquals(mapConfig, config.getMapConfig("xyz.abc@foo"));

        // we should not match this (ends-with)
        assertNotEquals(mapConfig, config.getMapConfig("abcD"));
        assertNotEquals(mapConfig, config.getMapConfig("abcD@foo"));
        assertNotEquals(mapConfig, config.getMapConfig("xyz.abcD"));
        assertNotEquals(mapConfig, config.getMapConfig("xyz.abcD@foo"));
    }
}
