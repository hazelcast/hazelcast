/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.jmx;

import com.hazelcast.config.Config;
import com.hazelcast.replicatedmap.ReplicatedMap;
import com.hazelcast.spi.properties.ClusterProperty;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ReplicatedMapMBeanTest extends HazelcastTestSupport {

    private static final String TYPE_NAME = "ReplicatedMap";

    private TestHazelcastInstanceFactory hazelcastInstanceFactory = createHazelcastInstanceFactory(1);
    private MBeanDataHolder holder;

    private ReplicatedMap<String, String> replicatedMap;
    private String objectName;

    @Before
    public void setUp() {
        Config config = new Config();
        config.setProperty(ClusterProperty.JMX_UPDATE_INTERVAL_SECONDS.getName(), "1");
        holder = new MBeanDataHolder(hazelcastInstanceFactory, config);
        replicatedMap = holder.getHz().getReplicatedMap("replicatedMap");
        objectName = replicatedMap.getName();

        holder.assertMBeanExistEventually(TYPE_NAME, replicatedMap.getName());
    }

    @After
    public void tearDown() {
        hazelcastInstanceFactory.shutdownAll();
    }

    @Test
    public void testName() throws Exception {
        String name = getStringAttribute("name");

        assertEquals("replicatedMap", name);
    }

    @Test
    public void testConfig() throws Exception {
        String config = getStringAttribute("config");

        assertTrue("configuration string should start with 'ReplicatedMapConfig{'", config.startsWith("ReplicatedMapConfig{"));
    }

    @Test
    public void testAttributesAndOperations() throws Exception {
        long started = System.currentTimeMillis();

        replicatedMap.put("firstKey", "firstValue");
        replicatedMap.put("secondKey", "secondValue");
        replicatedMap.remove("secondKey");
        replicatedMap.size();
        String value = replicatedMap.get("firstKey");

        long localEntryCount = getLongAttribute("localOwnedEntryCount");
        long localCreationTime = getLongAttribute("localCreationTime");
        long localLastAccessTime = getLongAttribute("localLastAccessTime");
        long localLastUpdateTime = getLongAttribute("localLastUpdateTime");
        long localHits = getLongAttribute("localHits");

        long localPutOperationCount = getLongAttribute("localPutOperationCount");
        long localGetOperationCount = getLongAttribute("localGetOperationCount");
        long localRemoveOperationCount = getLongAttribute("localRemoveOperationCount");

        long localTotalPutLatency = getLongAttribute("localTotalPutLatency");
        long localTotalGetLatency = getLongAttribute("localTotalGetLatency");
        long localTotalRemoveLatency = getLongAttribute("localTotalRemoveLatency");
        long localMaxPutLatency = getLongAttribute("localMaxPutLatency");
        long localMaxGetLatency = getLongAttribute("localMaxGetLatency");
        long localMaxRemoveLatency = getLongAttribute("localMaxRemoveLatency");

        long localEventOperationCount = getLongAttribute("localEventOperationCount");
        long localOtherOperationCount = getLongAttribute("localOtherOperationCount");
        long localTotal = getLongAttribute("localTotal");
        int size = getIntegerAttribute("size");

        assertEquals("firstValue", value);

        assertEquals(1, localEntryCount);
        long lowerBound = started - TimeUnit.SECONDS.toMillis(10);
        long upperBound = started + TimeUnit.SECONDS.toMillis(10);
        assertTrue(
                "localCreationTime <" + localCreationTime + "> has to be between [" + lowerBound + " and " + upperBound + "]",
                lowerBound < localCreationTime && localCreationTime < upperBound);
        assertTrue(
                "localLastAccessTime <" + localLastAccessTime + "> has to be between [" + lowerBound + " and " + upperBound + "]",
                lowerBound < localLastAccessTime && localLastAccessTime < upperBound);
        assertTrue(
                "localLastUpdateTime <" + localLastUpdateTime + "> has to be between [" + lowerBound + " and " + upperBound + "]",
                lowerBound < localLastUpdateTime && localLastUpdateTime < upperBound);
        assertEquals(1, localHits);

        assertEquals(2, localPutOperationCount);
        assertEquals(1, localGetOperationCount);
        assertEquals(1, localRemoveOperationCount);

        assertTrue("localTotalPutLatency should be >= 0", localTotalPutLatency >= 0);
        assertTrue("localTotalGetLatency should be >= 0", localTotalGetLatency >= 0);
        assertTrue("localTotalRemoveLatency should be >= 0", localTotalRemoveLatency >= 0);
        assertTrue("localMaxPutLatency should be >= 0", localMaxPutLatency >= 0);
        assertTrue("localMaxGetLatency should be >= 0", localMaxGetLatency >= 0);
        assertTrue("localMaxRemoveLatency should be >= 0", localMaxRemoveLatency >= 0);

        assertEquals(0, localEventOperationCount);
        assertTrue("localOtherOperationCount should be > 0", localOtherOperationCount > 0);
        assertTrue("localTotal should be > 0", localTotal > 0);
        assertEquals(1, size);

        holder.invokeMBeanOperation(TYPE_NAME, objectName, "clear", null, null);
        size = getIntegerAttribute("size");

        assertEquals(0, size);
    }

    @Test
    public void testAttributeHitsAndOwnedEntryCountUpdatedAfterInterval() throws Exception {
        String firstKey = "firstKey";
        String secondKey = "secondKey";
        replicatedMap.put(firstKey, "firstValue");
        replicatedMap.get(firstKey);
        final String localHitsName = "localHits";
        final String localOwnedEntryCountName = "localOwnedEntryCount";
        long localHits = getLongAttribute(localHitsName);
        long localEntryCount = getLongAttribute(localOwnedEntryCountName);
        replicatedMap.get(firstKey);
        replicatedMap.put(secondKey, "secondValue");
        long localHitsNotUpdated = getLongAttribute(localHitsName);
        long localEntryCountNotUpdated = getLongAttribute(localOwnedEntryCountName);
        assertEquals(1, localHits);
        assertEquals(1, localHitsNotUpdated);
        assertEquals(1, localEntryCount);
        assertEquals(1, localEntryCountNotUpdated);
        sleepAtLeastSeconds(1);
        assertTrueEventually(() -> {
            assertEquals(2, getLongAttribute(localHitsName).longValue());
            assertEquals(2, getLongAttribute(localOwnedEntryCountName).longValue());
        });
    }

    private String getStringAttribute(String name) throws Exception {
        return (String) holder.getMBeanAttribute(TYPE_NAME, objectName, name);
    }

    private Long getLongAttribute(String name) throws Exception {
        return (Long) holder.getMBeanAttribute(TYPE_NAME, objectName, name);
    }

    private Integer getIntegerAttribute(String name) throws Exception {
        return (Integer) holder.getMBeanAttribute(TYPE_NAME, objectName, name);
    }

    private String invokeMethod(String methodName) throws Exception {
        return (String) holder.invokeMBeanOperation(TYPE_NAME, objectName, methodName, null, null);
    }
}
