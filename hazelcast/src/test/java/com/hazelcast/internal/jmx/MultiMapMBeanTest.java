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

import com.hazelcast.multimap.MultiMap;
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
public class MultiMapMBeanTest extends HazelcastTestSupport {

    private static final String TYPE_NAME = "MultiMap";

    private TestHazelcastInstanceFactory factory;
    private MBeanDataHolder holder;
    private MultiMap<String, String> multiMap;
    private String mapName = randomString();

    @Before
    public void setup() {
        factory = createHazelcastInstanceFactory();
        holder = new MBeanDataHolder(factory);
        multiMap = holder.getHz().getMultiMap(mapName);
        holder.assertMBeanExistEventually(TYPE_NAME, mapName);
    }

    @After
    public void tearDown() {
        factory.terminateAll();
    }

    @Test
    public void testName() throws Exception {
        String name = getStringAttribute("name");

        assertEquals(mapName, name);
    }

    @Test
    public void testConfig() throws Exception {
        String config = getStringAttribute("config");

        assertTrue("configuration string should start with 'MultiMapConfig{'", config.startsWith("MultiMapConfig{"));
    }

    @Test
    public void testStats() throws Exception {
        long started = System.currentTimeMillis();
        int iterations = 100;
        for (int i = 0; i < iterations; i++) {
            multiMap.put("Ubuntu", "Artful Aardvark");
            multiMap.put("Ubuntu", "Zesty Zapus");
            multiMap.put("Fedora", "Heisenbug");
            multiMap.put("Fedora", "Schrodinger's Cat");

            multiMap.get("Windows");
            multiMap.get("Ubuntu");
            multiMap.remove("Fedora", "Heisenbug");
            multiMap.localKeySet();
        }

        long localEntryCount = getLongAttribute("localOwnedEntryCount");
        long localBackupEntryCount = getLongAttribute("localBackupEntryCount");
        int localBackupCount = getIntegerAttribute("localBackupCount");

        long localCreationTime = getLongAttribute("localCreationTime");
        long localLastAccessTime = getLongAttribute("localLastAccessTime");
        long localLastUpdateTime = getLongAttribute("localLastUpdateTime");
        long localHits = getLongAttribute("localHits");
        long localLockedEntryCount = getLongAttribute("localLockedEntryCount");
        long localDirtyEntryCount = getLongAttribute("localDirtyEntryCount");

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

        assertEquals(iterations, localHits);
        assertEquals(3, localEntryCount);
        assertEquals(0, localBackupEntryCount);
        assertEquals(1, localBackupCount);
        assertEquals(2 * iterations, localGetOperationCount);
        assertEquals(4 * iterations, localPutOperationCount);
        assertEquals(iterations, localRemoveOperationCount);
        assertEquals(0, localLockedEntryCount);
        assertEquals(0, localDirtyEntryCount);

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

        assertTrue("Total put latency should be >= 0", localTotalPutLatency >= 0);
        assertTrue("Total get latency should be >= 0", localTotalGetLatency >= 0);
        assertTrue("Total remove latency should be >= 0", localTotalRemoveLatency >= 0);
        assertTrue("Max put latency should be >= 0", localMaxPutLatency >= 0);
        assertTrue("Max get latency should be >= 0", localMaxGetLatency >= 0);
        assertTrue("Max remove latency should be >= 0", localMaxRemoveLatency >= 0);

        assertEquals(0, localEventOperationCount);
        assertEquals(iterations, localOtherOperationCount);
        assertTrue("Total operation count should be > 0", localTotal > 0);
        assertEquals(3, size);
    }

    private String getStringAttribute(String name) throws Exception {
        return (String) holder.getMBeanAttribute(TYPE_NAME, mapName, name);
    }

    private Long getLongAttribute(String name) throws Exception {
        return (Long) holder.getMBeanAttribute(TYPE_NAME, mapName, name);
    }

    private Integer getIntegerAttribute(String name) throws Exception {
        return (Integer) holder.getMBeanAttribute(TYPE_NAME, mapName, name);
    }

}
