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

package com.hazelcast.nio;

import com.hazelcast.config.JavaSerializationFilterConfig;
import com.hazelcast.internal.serialization.SerializationClassNameFilter;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

/**
 * Unit tests for {@link SerializationClassNameFilter}.
 */
@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class SerializationClassNameFilterTest {

    /**
     * <pre>
     * Given: Default configuration is used.
     * When: {@link SerializationClassNameFilter#filter(String)} is called for a java.lang class
     * Then: no exception is thrown as the java prefix is in the default whitelist
     * </pre>
     */
    @Test
    public void testDefaultPass() {
        JavaSerializationFilterConfig config = new JavaSerializationFilterConfig();
        new SerializationClassNameFilter(config).filter("java.lang.Object");
    }

    /**
     * <pre>
     * Given: Default configuration is used
     * When: {@link SerializationClassNameFilter#filter(String)} is called for a class name which doesn't fit the default whitelist.
     * Then: {@link SecurityException} is thrown
     * </pre>
     */
    @Test(expected = SecurityException.class)
    public void testDefaultFail() {
        new SerializationClassNameFilter(new JavaSerializationFilterConfig()).filter("bsh.XThis");
    }

    /**
     * <pre>
     * Given: Default is disabled and explicit whitelist is used.
     * When: {@link SerializationClassNameFilter#filter(String)} is called for a whitelisted class.
     * Then: no exception is thrown
     * </pre>
     */
    @Test
    public void testClassInWhitelist() {
        JavaSerializationFilterConfig config = new JavaSerializationFilterConfig().setDefaultsDisabled(true);
        config.getWhitelist().addClasses("java.lang.Test1", "java.lang.Test2", "java.lang.Test3");
        new SerializationClassNameFilter(config).filter("java.lang.Test2");
    }

    /**
     * <pre>
     * Given: Whitelist is set.
     * When: {@link SerializationClassNameFilter#filter(String)} is called for a class which has whitelisted package.
     * Then: no exception is thrown
     * </pre>
     */
    @Test
    public void testPackageInWhitelist() {
        JavaSerializationFilterConfig config = new JavaSerializationFilterConfig();
        config.getWhitelist().addPackages("com.whitelisted");
        new SerializationClassNameFilter(config).filter("com.whitelisted.Test2");
    }

    /**
     * <pre>
     * Given: Whitelist is set and defaults are disabled.
     * When: {@link SerializationClassNameFilter#filter(String)} is called for a not whitelisted class.
     * Then: {@link SecurityException} is thrown
     * </pre>
     */
    @Test(expected = SecurityException.class)
    public void testClassNotInWhitelist() {
        JavaSerializationFilterConfig config = new JavaSerializationFilterConfig().setDefaultsDisabled(true);
        config.getWhitelist().addClasses("java.lang.Test1", "java.lang.Test2", "java.lang.Test3");
        new SerializationClassNameFilter(config).filter("java.lang.Test4");
    }

    /**
     * <pre>
     * Given: Blacklist is used and defaults are enabled.
     * When: {@link SerializationClassNameFilter#filter(String)} is called for a class which is fits default whitelist
     *        but it's also blacklisted.
     * Then: {@link SecurityException} is thrown
     * </pre>
     */
    @Test(expected = SecurityException.class)
    public void testBlacklistedWithDefaultWhitelist() {
        JavaSerializationFilterConfig config = new JavaSerializationFilterConfig();
        config.getBlacklist().addClasses("java.lang.Test3", "java.lang.Test2", "java.lang.Test1");
        new SerializationClassNameFilter(config).filter("java.lang.Test1");
    }

    /**
     * <pre>
     * Given: Blacklist with prefix is used which overlaps default whitelist.
     * When: {@link SerializationClassNameFilter#filter(String)} is called for a class which fits default whitelist
     *        but it's also blacklisted.
     * Then: {@link SecurityException} is thrown
     * </pre>
     */
    @Test(expected = SecurityException.class)
    public void testBlacklistPrefix() {
        JavaSerializationFilterConfig config = new JavaSerializationFilterConfig();
        config.getBlacklist().addPrefixes("com.hazelcast.test");
        new SerializationClassNameFilter(config).filter("com.hazelcast.test.Test1");
    }
}
