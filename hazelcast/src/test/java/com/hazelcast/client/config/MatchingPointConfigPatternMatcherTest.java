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

package com.hazelcast.client.config;

import com.hazelcast.config.InvalidConfigurationException;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.config.QueueConfig;
import com.hazelcast.config.matcher.MatchingPointConfigPatternMatcher;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertNull;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class MatchingPointConfigPatternMatcherTest {

    @Test
    public void testNearCacheConfigWithoutWildcard() {
        NearCacheConfig nearCacheConfig = new NearCacheConfig().setName("someNearCache").setMaxIdleSeconds(1337);

        ClientConfig config = new ClientConfig();
        config.setConfigPatternMatcher(new MatchingPointConfigPatternMatcher());
        config.addNearCacheConfig(nearCacheConfig);

        assertEquals(nearCacheConfig, config.getNearCacheConfig("someNearCache"));

        // non-matching name
        assertNull(config.getNearCacheConfig("doesNotExist"));
        // non-matching case
        assertNull(config.getNearCacheConfig("SomeNearCache"));
    }

    @Test
    public void testNearCacheConfigWildcard1() {
        NearCacheConfig nearCacheConfig = new NearCacheConfig().setName("*hazelcast.test.myNearCache").setMaxIdleSeconds(13);

        ClientConfig config = new ClientConfig();
        config.setConfigPatternMatcher(new MatchingPointConfigPatternMatcher());
        config.addNearCacheConfig(nearCacheConfig);

        assertEquals(nearCacheConfig,
                config.getNearCacheConfig("com.hazelcast.test.myNearCache"));
    }

    @Test
    public void testNearCacheConfigWildcard2() {
        NearCacheConfig nearCacheConfig = new NearCacheConfig().setName("com.hazelcast.*.myNearCache").setMaxIdleSeconds(14);

        ClientConfig config = new ClientConfig();
        config.setConfigPatternMatcher(new MatchingPointConfigPatternMatcher());
        config.addNearCacheConfig(nearCacheConfig);

        assertEquals(nearCacheConfig,
                config.getNearCacheConfig("com.hazelcast.test.myNearCache"));
    }

    @Test
    public void testNearCacheConfigWildcard3() {
        NearCacheConfig nearCacheConfig = new NearCacheConfig().setName("com.hazelcast.test.*").setMaxIdleSeconds(15);

        ClientConfig config = new ClientConfig();
        config.setConfigPatternMatcher(new MatchingPointConfigPatternMatcher());
        config.addNearCacheConfig(nearCacheConfig);

        assertEquals(nearCacheConfig,
                config.getNearCacheConfig("com.hazelcast.test.myNearCache"));
    }

    @Test
    public void testNearCacheConfigWildcardMultipleConfigs() {
        NearCacheConfig nearCacheConfig1 = new NearCacheConfig().setName("com.hazelcast.*").setMaxIdleSeconds(1_001);
        NearCacheConfig nearCacheConfig2 = new NearCacheConfig().setName("com.hazelcast.test.*").setMaxIdleSeconds(1_002);
        NearCacheConfig nearCacheConfig3 = new NearCacheConfig().setName("com.hazelcast.test.sub.*").setMaxIdleSeconds(1_003);

        ClientConfig config = new ClientConfig();
        config.setConfigPatternMatcher(new MatchingPointConfigPatternMatcher());
        config.addNearCacheConfig(nearCacheConfig1);
        config.addNearCacheConfig(nearCacheConfig2);
        config.addNearCacheConfig(nearCacheConfig3);

        // we should get the best matching result
        assertEquals(nearCacheConfig1,
                config.getNearCacheConfig("com.hazelcast.myNearCache"));
        assertEquals(nearCacheConfig2,
                config.getNearCacheConfig("com.hazelcast.test.myNearCache"));
        assertEquals(nearCacheConfig3,
                config.getNearCacheConfig("com.hazelcast.test.sub.myNearCache"));
    }

    @Test(expected = InvalidConfigurationException.class)
    public void testMapConfigWildcardMultipleAmbiguousConfigs() {
        NearCacheConfig nearCacheConfig1 = new NearCacheConfig().setName("com.hazelcast*");
        NearCacheConfig nearCacheConfig2 = new NearCacheConfig().setName("*com.hazelcast");

        ClientConfig config = new ClientConfig();
        config.setConfigPatternMatcher(new MatchingPointConfigPatternMatcher());
        config.addNearCacheConfig(nearCacheConfig1);
        config.addNearCacheConfig(nearCacheConfig2);

        config.getNearCacheConfig("com.hazelcast");
    }

    @Test
    public void testNearCacheConfigWildcardMatchingPointStartsWith() {
        NearCacheConfig nearCacheConfig1 = new NearCacheConfig().setName("hazelcast.*");
        NearCacheConfig nearCacheConfig2 = new NearCacheConfig().setName("hazelcast.test.*");
        NearCacheConfig nearCacheConfig3 = new NearCacheConfig().setName("hazelcast.test.sub.*");

        ClientConfig config = new ClientConfig();
        config.setConfigPatternMatcher(new MatchingPointConfigPatternMatcher());
        config.addNearCacheConfig(nearCacheConfig1);
        config.addNearCacheConfig(nearCacheConfig2);
        config.addNearCacheConfig(nearCacheConfig3);

        // we should not match any of the configs (startsWith)
        assertNull(config.getNearCacheConfig("com.hazelcast.myNearCache"));
        assertNull(config.getNearCacheConfig("com.hazelcast.test.myNearCache"));
        assertNull(config.getNearCacheConfig("com.hazelcast.test.sub.myNearCache"));
    }

    @Test
    public void testNearCacheConfigWildcardMatchingPointEndsWith() {
        NearCacheConfig nearCacheConfig1 = new NearCacheConfig().setName("*.sub");
        NearCacheConfig nearCacheConfig2 = new NearCacheConfig().setName("*.test.sub");
        NearCacheConfig nearCacheConfig3 = new NearCacheConfig().setName("*.hazelcast.test.sub");

        ClientConfig config = new ClientConfig();
        config.setConfigPatternMatcher(new MatchingPointConfigPatternMatcher());
        config.addNearCacheConfig(nearCacheConfig1);
        config.addNearCacheConfig(nearCacheConfig2);
        config.addNearCacheConfig(nearCacheConfig3);

        // we should not match any of the configs (endsWith)
        assertNull(config.getNearCacheConfig("com.hazelFast.Fast.sub.myNearCache"));
        assertNull(config.getNearCacheConfig("hazelFast.test.sub.myNearCache"));
        assertNull(config.getNearCacheConfig("test.sub.myNearCache"));
    }

    @Test
    public void testNearCacheConfigWildcardOnly() {
        NearCacheConfig nearCacheConfig = new NearCacheConfig().setName("*").setMaxIdleSeconds(127);

        ClientConfig config = new ClientConfig();
        config.setConfigPatternMatcher(new MatchingPointConfigPatternMatcher());
        config.addNearCacheConfig(nearCacheConfig);

        assertEquals(nearCacheConfig,
                config.getNearCacheConfig("com.hazelcast.myNearCache"));
    }

    @Test
    public void testNearCacheConfigWildcardOnlyMultipleConfigs() {
        NearCacheConfig nearCacheConfig1 = new NearCacheConfig().setName("*").setMaxIdleSeconds(11);
        NearCacheConfig nearCacheConfig2 = new NearCacheConfig().setName("com.hazelcast.*").setMaxIdleSeconds(12);

        ClientConfig config = new ClientConfig();
        config.setConfigPatternMatcher(new MatchingPointConfigPatternMatcher());
        config.addNearCacheConfig(nearCacheConfig1);
        config.addNearCacheConfig(nearCacheConfig2);

        // we should get the best matching result
        assertEquals(nearCacheConfig2,
                config.getNearCacheConfig("com.hazelcast.myNearCache"));
    }

    public static void assertEquals(MapConfig expected, MapConfig actual) {
        assertThat(actual).isEqualToIgnoringGivenFields(expected, "name");
    }
    public static void assertEquals(QueueConfig expected, QueueConfig actual) {
        assertThat(actual).isEqualToIgnoringGivenFields(expected, "name");
    }
    public static void assertEquals(NearCacheConfig expected, NearCacheConfig actual) {
        assertThat(actual).isEqualToIgnoringGivenFields(expected, "name");
    }
}
