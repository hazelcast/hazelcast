/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.spi.discovery.multicast;

import com.hazelcast.config.Config;
import com.hazelcast.config.XmlConfigBuilder;
import com.hazelcast.config.properties.ValidationException;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestEnvironment;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.InputStream;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class MemberToMemberDiscoveryTest extends HazelcastTestSupport {

    private Config config;
    private TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);

    @Before
    public void setUp() {
        String xmlFileName = "hazelcast-multicast-plugin.xml";
        InputStream xmlResource = MulticastDiscoveryStrategy.class.getClassLoader().getResourceAsStream(xmlFileName);
        config = new XmlConfigBuilder(xmlResource).build();

        System.setProperty(TestEnvironment.HAZELCAST_TEST_USE_NETWORK, "true");
        System.setProperty("java.net.preferIPv4Stack", "true");
    }

    @After
    public void tearDown() {
        factory.shutdownAll();
    }

    @Test
    public void formClusterWithTwoMembersTest() throws InterruptedException {
        HazelcastInstance[] instances = factory.newInstances(config);
        assertClusterSizeEventually(2, instances[0]);
    }

    @Test(expected = ValidationException.class)
    public void invalidPortPropertyTest() throws InterruptedException {
        String xmlFileName = "hazelcast-multicast-plugin-invalid-port.xml";
        InputStream xmlResource = MulticastDiscoveryStrategy.class.getClassLoader().getResourceAsStream(xmlFileName);
        config = new XmlConfigBuilder(xmlResource).build();
        factory.newInstances(config);
    }

    @After
    public void shutdown() {
        factory.shutdownAll();
    }
}
