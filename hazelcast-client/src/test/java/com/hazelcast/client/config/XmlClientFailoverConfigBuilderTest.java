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
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.ByteArrayInputStream;
import java.net.URL;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class XmlClientFailoverConfigBuilderTest extends AbstractClientFailoverConfigBuilderTest {

    @Before
    public void init() throws Exception {
        URL schemaResource = XmlClientFailoverConfigBuilderTest.class.
                getClassLoader().getResource("hazelcast-client-failover-sample.xml");
        fullClientConfig = new XmlClientFailoverConfigBuilder(schemaResource).build();
    }

    @Test(expected = InvalidConfigurationException.class)
    public void testInvalidRootElement() {
        String xml = "<hazelcast>"
                + "<group>"
                + "<name>dev</name>"
                + "<password>clusterpass</password>"
                + "</group>"
                + "</hazelcast>";
        buildConfig(xml);
    }

    @Test(expected = InvalidConfigurationException.class)
    public void testExpectsAtLeastOneConfig() {
        String xml = "<hazelcast-client-failover>"
                + "    <clients>"
                + "    </clients>"
                + "</hazelcast-client-failover>";
        buildConfig(xml);
    }

    private static void buildConfig(String xml) {
        ByteArrayInputStream bis = new ByteArrayInputStream(xml.getBytes());
        XmlClientFailoverConfigBuilder configBuilder = new XmlClientFailoverConfigBuilder(bis);
        configBuilder.build();
    }

}
