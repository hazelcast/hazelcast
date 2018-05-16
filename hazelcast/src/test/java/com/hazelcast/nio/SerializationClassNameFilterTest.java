/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import com.hazelcast.config.JavaSerializationFilterConfig;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.QuickTest;

/**
 * Unit tests for {@link SerializationClassNameFilter}.
 */
@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class SerializationClassNameFilterTest {

    /**
     * <pre>
     * Given: Neither whitelist nor blacklist is configured.
     * When: {@link SerializationClassNameFilter#filter(String)} is called.
     * Then: no exception is thrown
     * </pre>
     */
    @Test
    public void testNoList() {
        JavaSerializationFilterConfig config = new JavaSerializationFilterConfig();
        new SerializationClassNameFilter(config).filter("java.lang.Object");
    }

    /**
     * <pre>
     * Given: Default blacklist is used.
     * When: {@link SerializationClassNameFilter#filter(String)} is called for a class name which is included in the default blacklist.
     * Then: {@link SecurityException} is thrown
     * </pre>
     */
    @Test(expected = SecurityException.class)
    public void testDefaultBlacklist() throws ClassNotFoundException {
        new SerializationClassNameFilter(new JavaSerializationFilterConfig()).filter("bsh.XThis");
    }

    /**
     * <pre>
     * Given: Default blacklist is used.
     * When: {@link SerializationClassNameFilter#filter(String)} is called for a class in package which is included in the default blacklist.
     * Then: {@link SecurityException} is thrown
     * </pre>
     */
    @Test(expected = SecurityException.class)
    public void testPackageDefaultBlacklisted() throws ClassNotFoundException {
        new SerializationClassNameFilter(new JavaSerializationFilterConfig()).filter("org.apache.commons.collections.functors.Test");
    }

    /**
     * <pre>
     * Given: Whitelist is set.
     * When: {@link SerializationClassNameFilter#filter(String)} is called for a whitelisted class.
     * Then: no exception is thrown
     * </pre>
     */
    @Test
    public void testClassInWhitelist() throws ClassNotFoundException {
        JavaSerializationFilterConfig config = new JavaSerializationFilterConfig();
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
    public void testPackageInWhitelist() throws ClassNotFoundException {
        JavaSerializationFilterConfig config = new JavaSerializationFilterConfig();
        config.getWhitelist().addPackages("com.whitelisted");
        new SerializationClassNameFilter(config).filter("com.whitelisted.Test2");
    }

    /**
     * <pre>
     * Given: Whitelist is set.
     * When: {@link SerializationClassNameFilter#filter(String)} is called for a not whitelisted class.
     * Then: {@link SecurityException} is thrown
     * </pre>
     */
    @Test(expected = SecurityException.class)
    public void testClassNotInWhitelist() throws ClassNotFoundException {
        JavaSerializationFilterConfig config = new JavaSerializationFilterConfig();
        config.getWhitelist().addClasses("java.lang.Test1", "java.lang.Test2", "java.lang.Test3");
        new SerializationClassNameFilter(config).filter("java.lang.Test4");
    }

    /**
     * <pre>
     * Given: Blacklist and Whitelist are set.
     * When: {@link SerializationClassNameFilter#filter(String)} is called for a class which is whitelisted and blacklisted together.
     * Then: {@link SecurityException} is thrown
     * </pre>
     */
    @Test(expected = SecurityException.class)
    public void testWhitelistedAndBlacklisted() throws ClassNotFoundException {
        JavaSerializationFilterConfig config = new JavaSerializationFilterConfig();
        config.getWhitelist().addClasses("java.lang.Test1", "java.lang.Test2", "java.lang.Test3");
        config.getBlacklist().addClasses("java.lang.Test3", "java.lang.Test2", "java.lang.Test1");
        new SerializationClassNameFilter(config).filter("java.lang.Test1");
    }
}