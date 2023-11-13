/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.config.cp.CPMapConfig;
import com.hazelcast.config.cp.CPSubsystemConfig;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import java.io.ByteArrayInputStream;

import static com.hazelcast.internal.util.RootCauseMatcher.rootCause;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

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

        assertThatThrownBy(() -> buildConfig(yaml))
                .has(rootCause(InvalidConfigurationException.class, "hazelcast/map/test"));
    }

    @Test
    public void testNullInSequenceThrows() {
        String yaml = ""
                + "hazelcast:\n"
                + "  listeners:\n"
                + "    - com.package.SomeListener\n"
                + "    -\n";

        assertThatThrownBy(() -> buildConfig(yaml))
                .has(rootCause(InvalidConfigurationException.class, "hazelcast/listeners"));
    }

    @Test
    public void testExplicitNullScalarThrows() {
        String yaml = ""
                + "hazelcast:\n"
                + "  instance-name: !!null";

        assertThatThrownBy(() -> buildConfig(yaml))
                .has(rootCause(InvalidConfigurationException.class, "hazelcast/instance-name"));
    }

    @Test
    public void testCPMapConfig() {
        String yaml = ""
                              + "hazelcast:\n"
                              + "  cp-subsystem:\n"
                              + "    maps:\n"
                              + "      map1:\n"
                              + "        max-size-mb: 50\n"
                              + "      map2:\n"
                              + "        max-size-mb: 25";
        Config config = buildConfig(yaml);
        assertNotNull(config);
        CPSubsystemConfig cpSubsystemConfig = config.getCPSubsystemConfig();
        assertEquals(2, cpSubsystemConfig.getCpMapConfigs().size());
        CPMapConfig map1Expected = new CPMapConfig("map1", 50);
        CPMapConfig map1Actual = cpSubsystemConfig.findCPMapConfig(map1Expected.getName());
        assertNotNull(map1Actual);
        assertEquals(map1Expected, map1Actual);
        CPMapConfig map2Expected = new CPMapConfig("map2", 25);
        CPMapConfig map2Actual = cpSubsystemConfig.findCPMapConfig(map2Expected.getName());
        assertNotNull(map2Actual);
        assertEquals(map2Expected, map2Actual);
    }

    private Config buildConfig(String yaml) {
        ByteArrayInputStream bis = new ByteArrayInputStream(yaml.getBytes());
        YamlConfigBuilder configBuilder = new YamlConfigBuilder(bis);
        return configBuilder.build();
    }
}
