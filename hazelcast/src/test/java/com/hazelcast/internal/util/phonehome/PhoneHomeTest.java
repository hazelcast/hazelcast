/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.config.*;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.BuildInfoProvider;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;
import java.lang.management.RuntimeMXBean;
import java.util.Map;
import java.util.UUID;

import static com.hazelcast.test.Accessors.getNode;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class PhoneHomeTest extends HazelcastTestSupport {

    private Node node;
    private PhoneHome phoneHome;

    @Before
    public void initialise() {
        HazelcastInstance hz = createHazelcastInstance();
        node = getNode(hz);
        phoneHome = new PhoneHome(node);
    }

    @Test
    public void testPhoneHomeParameters() {
        sleepAtLeastMillis(1);
        Map<String, String> parameters = phoneHome.phoneHome(true);
        RuntimeMXBean runtimeMxBean = ManagementFactory.getRuntimeMXBean();
        OperatingSystemMXBean osMxBean = ManagementFactory.getOperatingSystemMXBean();
        assertEquals(parameters.get("version"), BuildInfoProvider.getBuildInfo().getVersion());
        assertEquals(UUID.fromString(parameters.get("m")), node.getLocalMember().getUuid());
        assertNull(parameters.get("e"));
        assertNull(parameters.get("oem"));
        assertNull(parameters.get("l"));
        assertNull(parameters.get("hdgb"));
        assertEquals(parameters.get("p"), "source");
        assertEquals(parameters.get("crsz"), "A");
        assertEquals(parameters.get("cssz"), "A");
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
    }


    @Test
    public void testConvertToLetter() {
        assertEquals("A", MetricsCollector.convertToLetter(4));
        assertEquals("B", MetricsCollector.convertToLetter(9));
        assertEquals("C", MetricsCollector.convertToLetter(19));
        assertEquals("D", MetricsCollector.convertToLetter(39));
        assertEquals("E", MetricsCollector.convertToLetter(59));
        assertEquals("F", MetricsCollector.convertToLetter(99));
        assertEquals("G", MetricsCollector.convertToLetter(149));
        assertEquals("H", MetricsCollector.convertToLetter(299));
        assertEquals("J", MetricsCollector.convertToLetter(599));
        assertEquals("I", MetricsCollector.convertToLetter(1000));
    }

    @Test
    public void testMapCount() {
        Map<String, String> parameters = phoneHome.phoneHome(true);
        assertEquals(parameters.get("mpct"), "0");
        Map<String, String> map1 = node.hazelcastInstance.getMap("hazelcast");
        Map<String, String> map2 = node.hazelcastInstance.getMap("phonehome");
        parameters = phoneHome.phoneHome(true);
        assertEquals(parameters.get("mpct"), "2");
        Map<String, String> map3 = node.hazelcastInstance.getMap("maps");
        parameters = phoneHome.phoneHome(true);
        assertEquals(parameters.get("mpct"), "3");
    }

    @Test
    public void testMapCountWithBackupReadEnabled() {
        Map<String, String> parameters;
        parameters = phoneHome.phoneHome(true);
        assertEquals(parameters.get("mpbrct"), "0");

        Map<String, String> map1 = node.hazelcastInstance.getMap("hazelcast");
        parameters = phoneHome.phoneHome(true);
        assertEquals(parameters.get("mpbrct"), "0");

        node.getConfig().getMapConfig("hazelcast").setReadBackupData(true);
        parameters = phoneHome.phoneHome(true);
        assertEquals(parameters.get("mpbrct"), "1");
    }

    @Test
    public void testMapCountWithMapStoreEnabled() {
        Map<String, String> parameters;
        parameters = phoneHome.phoneHome(true);
        assertEquals(parameters.get("mpmsct"), "0");

        Map<String, String> map1 = node.hazelcastInstance.getMap("hazelcast");
        parameters = phoneHome.phoneHome(true);
        assertEquals(parameters.get("mpmsct"), "0");

        node.getConfig().getMapConfig("hazelcast").getMapStoreConfig().setEnabled(true);
        parameters = phoneHome.phoneHome(true);
        assertEquals(parameters.get("mpmsct"), "1");
    }

    @Test
    public void testMapCountWithAtleastOneQueryCache() {
        Map<String, String> parameters;
        parameters = phoneHome.phoneHome(true);
        assertEquals(parameters.get("mpaoqcct"), "0");

        Map<String, String> map1 = node.hazelcastInstance.getMap("hazelcast");
        parameters = phoneHome.phoneHome(true);
        assertEquals(parameters.get("mpaoqcct"), "0");

        QueryCacheConfig cacheConfig = new QueryCacheConfig();
        cacheConfig.setName("hazelcastconfig");
        node.getConfig().getMapConfig("hazelcast").addQueryCacheConfig(cacheConfig);
        parameters = phoneHome.phoneHome(true);
        assertEquals(parameters.get("mpaoqcct"), "1");

        cacheConfig = new QueryCacheConfig();
        cacheConfig.setName("hazelcastconfig2");
        node.getConfig().getMapConfig("hazelcast").addQueryCacheConfig(cacheConfig);
        parameters = phoneHome.phoneHome(true);
        assertEquals(parameters.get("mpaoqcct"), "1");

    }

    @Test
    public void testMapCountWithAtleastOneIndex() {
        Map<String, String> parameters;
        parameters = phoneHome.phoneHome(true);
        assertEquals(parameters.get("mpaoict"), "0");

        Map<String, String> map1 = node.hazelcastInstance.getMap("hazelcast");
        parameters = phoneHome.phoneHome(true);
        assertEquals(parameters.get("mpaoict"), "0");

        IndexConfig config = new IndexConfig(IndexType.SORTED, "hazelcast");
        node.getConfig().getMapConfig("hazelcast").getIndexConfigs().add(config);
        parameters = phoneHome.phoneHome(true);
        assertEquals(parameters.get("mpaoict"), "1");

        config = new IndexConfig(IndexType.HASH, "phonehome");
        node.getConfig().getMapConfig("hazelcast").getIndexConfigs().add(config);
        parameters = phoneHome.phoneHome(true);
        assertEquals(parameters.get("mpaoict"), "1");

    }

    @Test
    public void testMapCountWithHotRestartEnabled() {
        Map<String, String> parameters;
        parameters = phoneHome.phoneHome(true);
        assertEquals(parameters.get("mphect"), "0");

        Map<String, String> map1 = node.hazelcastInstance.getMap("hazelcast");
        parameters = phoneHome.phoneHome(true);
        assertEquals(parameters.get("mphect"), "0");

        node.getConfig().getMapConfig("hazelcast").getHotRestartConfig().setEnabled(true);
        parameters = phoneHome.phoneHome(true);
        assertEquals(parameters.get("mphect"), "1");

    }

    @Test
    public void testMapCountWithWANReplication() {
        Map<String, String> parameters;
        parameters = phoneHome.phoneHome(true);
        assertEquals(parameters.get("mpwact"), "0");

        Map<String, String> map1 = node.hazelcastInstance.getMap("hazelcast");
        parameters = phoneHome.phoneHome(true);
        assertEquals(parameters.get("mpwact"), "0");

        node.getConfig().getMapConfig("hazelcast").setWanReplicationRef(new WanReplicationRef());
        parameters = phoneHome.phoneHome(true);
        assertEquals(parameters.get("mpwact"), "1");

    }

    @Test
    public void testMapCountWithAtleastOneAttribute() {
        Map<String, String> parameters;
        parameters = phoneHome.phoneHome(true);
        assertEquals(parameters.get("mpaocct"), "0");

        Map<String, String> map1 = node.hazelcastInstance.getMap("hazelcast");
        parameters = phoneHome.phoneHome(true);
        assertEquals(parameters.get("mpaocct"), "0");

        node.getConfig().getMapConfig("hazelcast").getAttributeConfigs().add(new AttributeConfig());
        parameters = phoneHome.phoneHome(true);
        assertEquals(parameters.get("mpaocct"), "1");

        node.getConfig().getMapConfig("hazelcast").getAttributeConfigs().add(new AttributeConfig());
        parameters = phoneHome.phoneHome(true);
        assertEquals(parameters.get("mpaocct"), "1");

    }

}

