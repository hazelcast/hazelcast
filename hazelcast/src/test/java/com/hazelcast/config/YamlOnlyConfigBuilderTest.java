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

import com.hazelcast.internal.util.RootCauseMatcher;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import java.io.ByteArrayInputStream;

/**
 * Test cases specific only to YAML based configuration. The cases not
 * YAML specific should be added to {@link YamlConfigBuilderTest}.
 * <p>
 * This test class is expected to contain only <strong>extra</strong> test
 * cases over the ones defined in {@link YamlConfigBuilderTest} in order
 * to cover YAML specific cases where YAML configuration derives from the
 * XML configuration to allow usage of YAML-native constructs.
 *
 * @see YamlConfigBuilderTest
 */
@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class YamlOnlyConfigBuilderTest {

    @Rule
    public ExpectedException expected = ExpectedException.none();

    @Test(expected = InvalidConfigurationException.class)
    public void testMapQueryCachePredicateBothClassNameAndSql() {
        String yaml = ""
                + "hazelcast:\n"
                + "  map:\n"
                + "    test:\n"
                + "      query-caches:\n"
                + "        cache-name:\n"
                + "          predicate:\n"
                + "            class-name: com.hazelcast.examples.SimplePredicate\n"
                + "            sql: \"%age=40\"\n";

        buildConfig(yaml);
    }

    @Test(expected = InvalidConfigurationException.class)
    public void testMapQueryCachePredicateNeitherClassNameNorSql() {
        String yaml = ""
                + "hazelcast:\n"
                + "  map:\n"
                + "    test:\n"
                + "      query-caches:\n"
                + "        cache-name:\n"
                + "          predicate: {}\n";

        buildConfig(yaml);
    }

    @Test
    public void testNullInMapThrows() {
        String yaml = ""
                + "hazelcast:\n"
                + "  map:\n"
                + "    test:\n"
                + "    query-caches: {}\n";

        expected.expect(new RootCauseMatcher(InvalidConfigurationException.class, "hazelcast/map/test"));
        buildConfig(yaml);
    }

    @Test
    public void testNullInSequenceThrows() {
        String yaml = ""
                + "hazelcast:\n"
                + "  listeners:\n"
                + "    - com.package.SomeListener\n"
                + "    -\n";

        expected.expect(new RootCauseMatcher(InvalidConfigurationException.class, "hazelcast/listeners"));
        buildConfig(yaml);
    }

    @Test
    public void testExplicitNullScalarThrows() {
        String yaml = ""
                + "hazelcast:\n"
                + "  instance-name: !!null";

        expected.expect(new RootCauseMatcher(InvalidConfigurationException.class, "hazelcast/instance-name"));
        buildConfig(yaml);
    }

    private Config buildConfig(String yaml) {
        ByteArrayInputStream bis = new ByteArrayInputStream(yaml.getBytes());
        YamlConfigBuilder configBuilder = new YamlConfigBuilder(bis);
        return configBuilder.build();
    }
}
