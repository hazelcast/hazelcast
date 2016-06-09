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

package com.hazelcast.internal.plugin.multicast;

import com.hazelcast.config.Config;
import com.hazelcast.config.XmlConfigBuilder;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestEnvironment;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.InputStream;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class MemberToMemberDiscoveryTest extends HazelcastTestSupport {

    Config config;
    HazelcastInstance[] instances;

    @Before
    public void setup() {
        String xmlFileName = "hazelcast-multicast-plugin.xml";
        InputStream xmlResource = MulticastDiscoveryStrategy.class.getClassLoader().getResourceAsStream(xmlFileName);
        config = new XmlConfigBuilder(xmlResource).build();
    }

    @Test
    public void formClusterWithTwoMembersTest() throws InterruptedException {
        System.setProperty(TestEnvironment.HAZELCAST_TEST_USE_NETWORK, "true");
        System.setProperty("java.net.preferIPv4Stack", "true");
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        instances = factory.newInstances(config);
        assertClusterSizeEventually(2, instances[0]);
        factory.shutdownAll();
    }

}
