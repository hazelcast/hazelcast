/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.util.RootCauseMatcher;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import java.io.ByteArrayInputStream;
import java.net.URL;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class YamlClientFailoverConfigBuilderTest extends AbstractClientFailoverConfigBuilderTest {

    @Rule
    public ExpectedException expected = ExpectedException.none();

    @Before
    public void init() throws Exception {
        URL schemaResource = YamlClientFailoverConfigBuilderTest.class.getClassLoader()
                                                                      .getResource("hazelcast-client-failover-sample.yaml");
        fullClientConfig = new YamlClientFailoverConfigBuilder(schemaResource).build();
    }

    @Test(expected = InvalidConfigurationException.class)
    public void testInvalidRootElement() {
        String yaml = ""
                + "hazelcast:\n"
                + "  group:\n"
                + "    name: dev\n"
                + "    password: clusterpass";
        buildConfig(yaml);
    }

    @Test
    public void testExpectsAtLeastOneConfig() {
        String yaml = ""
                + "hazelcast-client-failover:\n"
                + "  clients: {}";

        expected.expect(new RootCauseMatcher(InvalidConfigurationException.class, "hazelcast-client-failover/clients"));
        buildConfig(yaml);
    }

    private static void buildConfig(String yaml) {
        ByteArrayInputStream bis = new ByteArrayInputStream(yaml.getBytes());
        YamlClientFailoverConfigBuilder configBuilder = new YamlClientFailoverConfigBuilder(bis);
        configBuilder.build();
    }
}
