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

package com.hazelcast.util;

import com.hazelcast.config.Config;
import com.hazelcast.config.ManagementCenterConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.Node;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;
import java.lang.management.RuntimeMXBean;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class PhoneHomeTest extends HazelcastTestSupport {

    @Test
    public void testPhoneHomeParameters() {
        HazelcastInstance hz = createHazelcastInstance();
        Node node = getNode(hz);
        PhoneHome phoneHome = new PhoneHome();

        sleepAtLeastMillis(1);
        Map<String, String> parameters = phoneHome.phoneHome(node, "test_version", false);
        RuntimeMXBean runtimeMxBean = ManagementFactory.getRuntimeMXBean();
        OperatingSystemMXBean osMxBean = ManagementFactory.getOperatingSystemMXBean();
        assertEquals(parameters.get("version"), "test_version");
        assertEquals(parameters.get("m"), node.getLocalMember().getUuid());
        assertEquals(parameters.get("e"), "false");
        assertEquals(parameters.get("l"), "");
        assertEquals(parameters.get("p"), "source");
        assertEquals(parameters.get("crsz"), "A");
        assertEquals(parameters.get("cssz"), "A");
        assertEquals(parameters.get("hdgb"), "0");
        assertEquals(parameters.get("ccpp"), "0");
        assertEquals(parameters.get("cdn"), "0");
        assertEquals(parameters.get("cjv"), "0");
        assertEquals(parameters.get("cnjs"), "0");
        assertEquals(parameters.get("cpy"), "0");
        assertEquals(parameters.get("cgo"), "0");
        assertEquals(parameters.get("jetv"), "");
        assertFalse(Integer.parseInt(parameters.get("cuptm")) < 0);
        assertNotEquals(parameters.get("nuptm"), "0");
        assertNotEquals(parameters.get("nuptm"), parameters.get("cuptm"));
        assertEquals(parameters.get("osn"), osMxBean.getName());
        assertEquals(parameters.get("osa"), osMxBean.getArch());
        assertEquals(parameters.get("osv"), osMxBean.getVersion());
        assertEquals(parameters.get("jvmn"), runtimeMxBean.getVmName());
        assertEquals(parameters.get("jvmv"), System.getProperty("java.version"));
        assertEquals(parameters.get("mcver"), "MC_NOT_CONFIGURED");
        assertEquals(parameters.get("mclicense"), "MC_NOT_CONFIGURED");
    }

    @Test
    public void testPhoneHomeParameters_withManagementCenterConfiguredButNotAvailable() {
        ManagementCenterConfig managementCenterConfig = new ManagementCenterConfig()
                .setEnabled(true)
                .setUrl("http://localhost:11111/mancen");
        Config config = new Config()
                .setManagementCenterConfig(managementCenterConfig);

        HazelcastInstance hz = createHazelcastInstance(config);
        Node node = getNode(hz);
        PhoneHome phoneHome = new PhoneHome();

        sleepAtLeastMillis(1);
        Map<String, String> parameters = phoneHome.phoneHome(node, "test_version", false);
        assertEquals(parameters.get("mcver"), "MC_NOT_AVAILABLE");
        assertEquals(parameters.get("mclicense"), "MC_NOT_AVAILABLE");
    }

    @Test
    @SuppressWarnings("deprecation")
    public void testScheduling_whenVersionCheckIsDisabled() {
        Config config = new Config()
                .setProperty(GroupProperty.VERSION_CHECK_ENABLED.getName(), "false");

        HazelcastInstance hz = createHazelcastInstance(config);
        Node node = getNode(hz);

        PhoneHome phoneHome = new PhoneHome();
        phoneHome.check(node, "test_version", false);
        assertNull(phoneHome.phoneHomeFuture);
    }

    @Test
    public void testScheduling_whenPhoneHomeIsDisabled() {
        Config config = new Config()
                .setProperty(GroupProperty.PHONE_HOME_ENABLED.getName(), "false");

        HazelcastInstance hz = createHazelcastInstance(config);
        Node node = getNode(hz);

        PhoneHome phoneHome = new PhoneHome();
        phoneHome.check(node, "test_version", false);
        assertNull(phoneHome.phoneHomeFuture);
    }

    @Test
    public void testShutdown() {
        Config config = new Config()
                .setProperty(GroupProperty.PHONE_HOME_ENABLED.getName(), "true");

        HazelcastInstance hz = createHazelcastInstance(config);
        Node node = getNode(hz);

        PhoneHome phoneHome = new PhoneHome();
        phoneHome.check(node, "test_version", false);
        assertNotNull(phoneHome.phoneHomeFuture);
        assertFalse(phoneHome.phoneHomeFuture.isDone());
        assertFalse(phoneHome.phoneHomeFuture.isCancelled());

        phoneHome.shutdown();
        assertTrue(phoneHome.phoneHomeFuture.isCancelled());
    }

    @Test
    public void testConvertToLetter() {
        PhoneHome phoneHome = new PhoneHome();
        assertEquals("A", phoneHome.convertToLetter(4));
        assertEquals("B", phoneHome.convertToLetter(9));
        assertEquals("C", phoneHome.convertToLetter(19));
        assertEquals("D", phoneHome.convertToLetter(39));
        assertEquals("E", phoneHome.convertToLetter(59));
        assertEquals("F", phoneHome.convertToLetter(99));
        assertEquals("G", phoneHome.convertToLetter(149));
        assertEquals("H", phoneHome.convertToLetter(299));
        assertEquals("J", phoneHome.convertToLetter(599));
        assertEquals("I", phoneHome.convertToLetter(1000));
    }
}
