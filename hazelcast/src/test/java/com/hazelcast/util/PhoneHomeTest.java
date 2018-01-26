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

package com.hazelcast.util;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.Node;
import com.hazelcast.instance.TestUtil;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
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

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class PhoneHomeTest extends HazelcastTestSupport {

    private TestHazelcastInstanceFactory factory;
    private HazelcastInstance hz1;

    @After
    public void cleanup() {
        factory.terminateAll();
    }

    @Before
    public void init() {
        factory = new TestHazelcastInstanceFactory(2);
        hz1 = factory.newHazelcastInstance();
    }

    @Test
    public void testPhoneHomeParameters() throws Exception {
        Node node1 = TestUtil.getNode(hz1);
        PhoneHome phoneHome = new PhoneHome();
        sleepAtLeastMillis(1);
        Map<String, String> parameters = phoneHome.phoneHome(node1, "test_version", false);
        RuntimeMXBean runtimeMxBean = ManagementFactory.getRuntimeMXBean();
        OperatingSystemMXBean osMxBean = ManagementFactory.getOperatingSystemMXBean();

        assertEquals(parameters.get("version"), "test_version");
        assertEquals(parameters.get("m"), node1.getLocalMember().getUuid());
        assertEquals(parameters.get("e"), "false");
        assertEquals(parameters.get("l"), "NULL");
        assertEquals(parameters.get("p"), "source");
        assertEquals(parameters.get("crsz"), "A");
        assertEquals(parameters.get("cssz"), "A");
        assertEquals(parameters.get("hdgb"), "0");
        assertEquals(parameters.get("ccpp"), "0");
        assertEquals(parameters.get("cdn"), "0");
        assertEquals(parameters.get("cjv"), "0");
        assertEquals(parameters.get("cnjs"), "0");
        assertEquals(parameters.get("cpy"), "0");
        assertEquals(parameters.get("jetv"), "");
        assertFalse(Integer.parseInt(parameters.get("cuptm")) < 0);
        assertNotEquals(parameters.get("nuptm"), "0");
        assertNotEquals(parameters.get("nuptm"), parameters.get("cuptm"));
        assertEquals(parameters.get("osn"), osMxBean.getName());
        assertEquals(parameters.get("osa"), osMxBean.getArch());
        assertEquals(parameters.get("osv"), osMxBean.getVersion());
        assertEquals(parameters.get("jvmn"), runtimeMxBean.getVmName());
        assertEquals(parameters.get("jvmv"), System.getProperty("java.version"));
    }

    @Test
    public void testConvertToLetter() throws Exception {

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
