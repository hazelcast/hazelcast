/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.config;

import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.config.InMemoryFormat.BINARY;
import static com.hazelcast.config.InMemoryFormat.NATIVE;
import static com.hazelcast.config.InMemoryFormat.OBJECT;
import static com.hazelcast.internal.config.ConfigValidator.checkMapConfig;
import static com.hazelcast.internal.config.ConfigValidator.checkNearCacheConfig;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class ConfigValidatorTest extends HazelcastTestSupport {

    @Test
    public void testConstructor() {
        assertUtilityConstructor(ConfigValidator.class);
    }

    @Test
    public void test_checkMapConfig_BINARY() {
        checkMapConfig(getMapConfig(BINARY));
    }

    @Test
    public void test_checkMapConfig_OBJECT() {
        checkMapConfig(getMapConfig(OBJECT));
    }

    /**
     * Not supported in open source version, so test is expected to throw exception.
     */
    @Test(expected = IllegalArgumentException.class)
    public void test_checkMapConfig_NATIVE() {
        checkMapConfig(getMapConfig(NATIVE));
    }

    @Test
    @SuppressWarnings("deprecation")
    public void test_checkMapConfig_withIgnoredConfigMinEvictionCheckMillis() {
        MapConfig mapConfig = getMapConfig(BINARY)
                .setMinEvictionCheckMillis(100);
        checkMapConfig(mapConfig);
    }

    @Test
    @SuppressWarnings("deprecation")
    public void test_checkMapConfig_withIgnoredConfigEvictionPercentage() {
        MapConfig mapConfig = getMapConfig(BINARY)
                .setEvictionPercentage(50);
        checkMapConfig(mapConfig);
    }

    @Test
    public void test_checkNearCacheConfig_BINARY() {
        checkNearCacheConfig(getNearCacheConfig(BINARY), false);
    }

    @Test
    public void test_checkNearCacheConfig_OBJECT() {
        checkNearCacheConfig(getNearCacheConfig(OBJECT), false);
    }

    /**
     * Not supported in open source version, so test is expected to throw exception.
     */
    @Test(expected = IllegalArgumentException.class)
    public void test_checkNearCacheConfig_NATIVE() {
        checkNearCacheConfig(getNearCacheConfig(NATIVE), false);
    }

    /**
     * Not supported client configuration, so test is expected to throw exception.
     */
    @Test(expected = IllegalArgumentException.class)
    public void test_checkNearCacheConfig_withUnsupportedClientConfig() {
        checkNearCacheConfig(getNearCacheConfig(BINARY), true);
    }

    private MapConfig getMapConfig(InMemoryFormat inMemoryFormat) {
        return new MapConfig()
                .setInMemoryFormat(inMemoryFormat);
    }

    private NearCacheConfig getNearCacheConfig(InMemoryFormat inMemoryFormat) {
        return new NearCacheConfig()
                .setInMemoryFormat(inMemoryFormat)
                .setCacheLocalEntries(true);
    }
}
