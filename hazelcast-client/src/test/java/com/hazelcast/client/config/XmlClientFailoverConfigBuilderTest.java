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
import com.hazelcast.core.HazelcastException;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.ByteArrayInputStream;
import java.net.URL;
import java.util.List;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class XmlClientFailoverConfigBuilderTest {

    private ClientFailoverConfig fullClientConfig;

    @Before
    public void init() throws Exception {
        URL schemaResource = XmlClientFailoverConfigBuilderTest.class.
                getClassLoader().getResource("hazelcast-client-failover-sample.xml");
        fullClientConfig = new XmlClientFailoverConfigBuilder(schemaResource).build();
    }

    @After
    @Before
    public void beforeAndAfter() {
        System.clearProperty("hazelcast.client.failover.config");
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


    @Test(expected = HazelcastException.class)
    public void loadingThroughSystemProperty_nonExistingFile() {
        System.setProperty("hazelcast.client.failover.config", "idontexist");
        new XmlClientFailoverConfigBuilder();
    }

    @Test(expected = HazelcastException.class)
    public void loadingThroughSystemProperty_nonExistingClasspathResource() {
        System.setProperty("hazelcast.client.failover.config", "classpath:idontexist");
        new XmlClientFailoverConfigBuilder();
    }

    @Test
    public void loadingThroughSystemProperty_existingClasspathResource() {
        System.setProperty("hazelcast.client.failover.config", "classpath:hazelcast-client-failover-sample.xml");

        XmlClientFailoverConfigBuilder configBuilder = new XmlClientFailoverConfigBuilder();
        ClientFailoverConfig config = configBuilder.build();
        assertEquals(2, config.getClientConfigs().size());
        assertEquals("cluster1", config.getClientConfigs().get(0).getGroupConfig().getName());
        assertEquals("cluster2", config.getClientConfigs().get(1).getGroupConfig().getName());
        assertEquals(4, config.getTryCount());
    }

    @Test
    public void testClientFailoverConfig() {
        List<ClientConfig> clientConfigs = fullClientConfig.getClientConfigs();
        assertEquals(2, clientConfigs.size());
        assertEquals("cluster1", clientConfigs.get(0).getGroupConfig().getName());
        assertEquals("cluster2", clientConfigs.get(1).getGroupConfig().getName());
        assertEquals(4, fullClientConfig.getTryCount());
    }

}
