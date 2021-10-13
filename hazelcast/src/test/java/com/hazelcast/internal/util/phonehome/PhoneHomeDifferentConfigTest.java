/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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
package com.hazelcast.internal.util.phonehome;

import com.hazelcast.config.Config;
import com.hazelcast.config.cp.CPSubsystemConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.spi.properties.ClusterProperty;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Map;

import static java.lang.System.getenv;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeFalse;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class PhoneHomeDifferentConfigTest extends HazelcastTestSupport {

    @Test
    public void testScheduling_whenPhoneHomeIsDisabled() {
        Config config = new Config()
                .setProperty(ClusterProperty.PHONE_HOME_ENABLED.getName(), "false");

        HazelcastInstance hz = createHazelcastInstance(config);
        Node node = getNode(hz);

        PhoneHome phoneHome = new PhoneHome(node);

        phoneHome.check();
        assertNull(phoneHome.phoneHomeFuture);
    }

    @Test
    public void testShutdown() {
        assumeFalse("Skipping. The PhoneHome is disabled by the Environment variable",
                "false".equals(getenv("HZ_PHONE_HOME_ENABLED")));
        Config config = new Config()
                .setProperty(ClusterProperty.PHONE_HOME_ENABLED.getName(), "true");

        HazelcastInstance hz = createHazelcastInstance(config);
        Node node = getNode(hz);

        PhoneHome phoneHome = new PhoneHome(node);
        phoneHome.check();
        assertNotNull(phoneHome.phoneHomeFuture);
        assertFalse(phoneHome.phoneHomeFuture.isDone());
        assertFalse(phoneHome.phoneHomeFuture.isCancelled());

        phoneHome.shutdown();
        assertTrue(phoneHome.phoneHomeFuture.isCancelled());
    }

    @Test
    public void testCPSubsystemEnabled() {
        Config config = new Config()
                .setCPSubsystemConfig(new CPSubsystemConfig()
                        .setCPMemberCount(3));

        HazelcastInstance[] hazelcastInstances = createHazelcastInstances(config, 3);
        Node node = getNode(hazelcastInstances[0]);

        PhoneHome phoneHome = new PhoneHome(node);

        Map<String, String> parameters = phoneHome.phoneHome(true);
        assertEquals("true", parameters.get(PhoneHomeMetrics.CP_SUBSYSTEM_ENABLED.getRequestParameterName()));
    }
}
