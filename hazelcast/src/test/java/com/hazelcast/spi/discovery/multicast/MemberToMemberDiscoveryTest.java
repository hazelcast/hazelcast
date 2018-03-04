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

package com.hazelcast.spi.discovery.multicast;

import com.hazelcast.config.Config;
import com.hazelcast.config.XmlConfigBuilder;
import com.hazelcast.config.properties.ValidationException;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.OverridePropertyRule;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.InputStream;

import static com.hazelcast.test.OverridePropertyRule.set;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class MemberToMemberDiscoveryTest extends HazelcastTestSupport {

    @Rule
    public final OverridePropertyRule overrideJoinWaitSecondsRule = set("hazelcast.wait.seconds.before.join", "10");
    @Rule
    public final OverridePropertyRule overridePreferIpv4Rule = set("java.net.preferIPv4Stack", "true");

    private Config config;

    @Before
    public void setUp() {
        String xmlFileName = "hazelcast-multicast-plugin.xml";
        InputStream xmlResource = MulticastDiscoveryStrategy.class.getClassLoader().getResourceAsStream(xmlFileName);
        config = new XmlConfigBuilder(xmlResource).build();
    }

    @After
    public void tearDown() {
        Hazelcast.shutdownAll();
    }

    @Test
    public void formClusterWithTwoMembersTest() {
        HazelcastInstance instance = Hazelcast.newHazelcastInstance(config);
        Hazelcast.newHazelcastInstance(config);
        assertClusterSizeEventually(2, instance);
    }

    @Test(expected = ValidationException.class)
    public void invalidPortPropertyTest() {
        String xmlFileName = "hazelcast-multicast-plugin-invalid-port.xml";
        InputStream xmlResource = MulticastDiscoveryStrategy.class.getClassLoader().getResourceAsStream(xmlFileName);
        config = new XmlConfigBuilder(xmlResource).build();

        Hazelcast.newHazelcastInstance(config);
    }
}
