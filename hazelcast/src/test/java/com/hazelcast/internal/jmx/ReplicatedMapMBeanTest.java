package com.hazelcast.internal.jmx;

import com.hazelcast.core.ReplicatedMap;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class ReplicatedMapMBeanTest extends HazelcastTestSupport {

    private static final String TYPE_NAME = "ReplicatedMap";

    private TestHazelcastInstanceFactory hazelcastInstanceFactory = createHazelcastInstanceFactory(1);
    private MBeanDataHolder holder = new MBeanDataHolder(hazelcastInstanceFactory);

    private ReplicatedMap<String, String> replicatedMap;
    private String objectName;

    @Before
    public void setUp() {
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
        String value = replicatedMap.get("firstKey");
        String values = invokeMethod("values");
        String entries = invokeMethod("entrySet");

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
        assertEquals("[firstValue,]", values);
        assertEquals("[{key:firstKey, value:firstValue},]", entries);

        assertEquals(1, localEntryCount);
        assertTrue(localCreationTime >= started);
        assertTrue(localLastAccessTime >= started);
        assertTrue(localLastUpdateTime >= started);
        assertEquals(3, localHits);

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
        values = invokeMethod("values");
        entries = invokeMethod("entrySet");
        size = getIntegerAttribute("size");

        assertEquals("Empty", values);
        assertEquals("Empty", entries);
        assertEquals(0, size);
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
