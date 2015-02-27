package com.hazelcast.map;

import com.eclipsesource.json.JsonObject;
import com.hazelcast.config.Config;
import com.hazelcast.config.MemberGroupConfig;
import com.hazelcast.config.PartitionGroupConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.core.MultiMap;
import com.hazelcast.monitor.LocalMapStats;
import com.hazelcast.monitor.NearCacheStats;
import com.hazelcast.monitor.impl.LocalMapStatsImpl;
import com.hazelcast.monitor.impl.NearCacheStatsImpl;
import com.hazelcast.nio.Address;
import com.hazelcast.partition.InternalPartitionService;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.util.JsonUtil;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Collection;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.util.JsonUtil.getLong;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class LocalMapStatsTest extends HazelcastTestSupport {

    private final String name = "fooMap";

    @Test
    public void testPutAndGetExpectHitsGenerated() throws Exception {
        HazelcastInstance h1 = createHazelcastInstance();
        IMap<Integer, Integer> map = h1.getMap("hits");
        for (int i = 0; i < 100; i++) {
            map.put(i, i);
            map.get(i);
        }
        LocalMapStats localMapStats = map.getLocalMapStats();
        assertEquals(100, localMapStats.getHits());
        assertEquals(100, localMapStats.getPutOperationCount());
        assertEquals(100, localMapStats.getGetOperationCount());
        localMapStats = map.getLocalMapStats(); // to ensure stats are recalculated correct.
        assertEquals(100, localMapStats.getHits());
        assertEquals(100, localMapStats.getPutOperationCount());
        assertEquals(100, localMapStats.getGetOperationCount());
    }

    @Test
    public void testLastAccessTime() throws InterruptedException {
        final TimeUnit timeUnit = TimeUnit.NANOSECONDS;
        final long startTime = timeUnit.toMillis(System.nanoTime());

        HazelcastInstance h1 = createHazelcastInstance();
        IMap<String, String> map1 = h1.getMap(name);

        String key = "key";
        map1.put(key, "value");

        long lastUpdateTime = map1.getLocalMapStats().getLastUpdateTime();
        assertTrue(lastUpdateTime >= startTime);

        Thread.sleep(5);
        map1.put(key, "value2");
        long lastUpdateTime2 = map1.getLocalMapStats().getLastUpdateTime();
        assertTrue(lastUpdateTime2 > lastUpdateTime);
    }

    @Test
    public void testEvictAll() throws Exception {
        HazelcastInstance h1 = createHazelcastInstance();
        IMap<String, String> map = h1.getMap(name);
        map.put("key", "value");
        map.evictAll();

        final long heapCost = map.getLocalMapStats().getHeapCost();

        assertEquals(0L, heapCost);
    }

    @Test
    public void testHits_whenMultipleNodes() throws InterruptedException {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        final HazelcastInstance[] instances = factory.newInstances();
        MultiMap<Object, Object> multiMap0 = instances[0].getMultiMap("testHits_whenMultipleNodes");
        MultiMap<Object, Object> multiMap1 = instances[1].getMultiMap("testHits_whenMultipleNodes");

        // InternalPartitionService is used in order to determine owners of these keys that we will use.
        InternalPartitionService partitionService = getNode(instances[0]).getPartitionService();
        Address address = partitionService.getPartitionOwner(partitionService.getPartitionId("test1"));

        boolean inFirstInstance = address.equals(getNode(instances[0]).getThisAddress());
        multiMap0.get("test0");

        multiMap0.put("test1", 1);
        multiMap1.get("test1");

        assertEquals(inFirstInstance ? 1 : 0, multiMap0.getLocalMultiMapStats().getHits());
        assertEquals(inFirstInstance ? 0 : 1, multiMap1.getLocalMultiMapStats().getHits());

        multiMap0.get("test1");
        multiMap1.get("test1");
        assertEquals(inFirstInstance ? 0 : 3, multiMap1.getLocalMultiMapStats().getHits());
    }

    @Test
    public void test_localMapStatsToJson() {
        final LocalMapStatsImpl localMapStats = new LocalMapStatsImpl();

        localMapStats.setLastAccessTime(1);
        localMapStats.setLastUpdateTime(2);
        localMapStats.incrementReceivedEvents();
        localMapStats.incrementOtherOperations();
        localMapStats.incrementGets(3);
        localMapStats.incrementPuts(4);
        localMapStats.incrementRemoves(5);

        localMapStats.setOwnedEntryCount(6);
        localMapStats.setBackupEntryCount(7);
        localMapStats.incrementOwnedEntryMemoryCost(8);
        localMapStats.incrementBackupEntryMemoryCost(9);
        localMapStats.incrementHeapCost(10);
        localMapStats.setLockedEntryCount(11);
        localMapStats.setDirtyEntryCount(12);
        localMapStats.setBackupCount(13);
        localMapStats.setHits(14);

        final NearCacheStatsImpl nearCacheStats = new NearCacheStatsImpl();
        nearCacheStats.setOwnedEntryCount(15);
        nearCacheStats.setOwnedEntryMemoryCost(16);
        nearCacheStats.incrementHits();
        nearCacheStats.incrementMisses();
        localMapStats.setNearCacheStats(nearCacheStats);

        final JsonObject json = localMapStats.toJson();

        assertEquals(getLong(json, "creationTime"), localMapStats.getCreationTime());
        assertEquals(getLong(json, "lastAccessTime"), localMapStats.getLastAccessTime());
        assertEquals(getLong(json, "lastUpdateTime"), localMapStats.getLastUpdateTime());
        assertEquals(getLong(json, "numberOfEvents"), localMapStats.getEventOperationCount());
        assertEquals(getLong(json, "numberOfOtherOperations"), localMapStats.getOtherOperationCount());
        assertEquals(getLong(json, "getCount"), localMapStats.getGetOperationCount());
        assertEquals(getLong(json, "putCount"), localMapStats.getPutOperationCount());
        assertEquals(getLong(json, "removeCount"), localMapStats.getRemoveOperationCount());
        assertEquals(getLong(json, "totalGetLatencies"), localMapStats.getTotalGetLatency());
        assertEquals(getLong(json, "totalPutLatencies"), localMapStats.getTotalPutLatency());
        assertEquals(getLong(json, "totalRemoveLatencies"), localMapStats.getTotalRemoveLatency());
        assertEquals(getLong(json, "maxGetLatency"), localMapStats.getMaxGetLatency());
        assertEquals(getLong(json, "maxPutLatency"), localMapStats.getMaxPutLatency());
        assertEquals(getLong(json, "maxRemoveLatency"), localMapStats.getMaxRemoveLatency());

        assertEquals(getLong(json, "ownedEntryCount"), localMapStats.getOwnedEntryCount());
        assertEquals(getLong(json, "backupEntryCount"), localMapStats.getBackupEntryCount());
        assertEquals(getLong(json, "ownedEntryMemoryCost"), localMapStats.getOwnedEntryMemoryCost());
        assertEquals(getLong(json, "backupEntryMemoryCost"), localMapStats.getBackupEntryMemoryCost());
        assertEquals(getLong(json, "heapCost"), localMapStats.getHeapCost());
        assertEquals(getLong(json, "lockedEntryCount"), localMapStats.getLockedEntryCount());
        assertEquals(getLong(json, "dirtyEntryCount"), localMapStats.getDirtyEntryCount());
        assertEquals(getLong(json, "backupCount"), localMapStats.getBackupCount());
        assertEquals(getLong(json, "hits"), localMapStats.getHits());

        final JsonObject nearCacheStatsJson = JsonUtil.getObject(json, "nearCacheStats");
        assertEquals(getLong(nearCacheStatsJson, "ownedEntryCount"), nearCacheStats.getOwnedEntryCount());
        assertEquals(getLong(nearCacheStatsJson, "ownedEntryMemoryCost"), nearCacheStats.getOwnedEntryMemoryCost());
        assertEquals(getLong(nearCacheStatsJson, "creationTime"), nearCacheStats.getCreationTime());
        assertEquals(getLong(nearCacheStatsJson, "hits"), nearCacheStats.getHits());
        assertEquals(getLong(nearCacheStatsJson, "misses"), nearCacheStats.getMisses());
    }

    @Test
    public void test_jsonToLocalMapStats() {
        final JsonObject json = new JsonObject();
        json.add("creationTime", 1L);
        json.add("lastAccessTime", 2L);
        json.add("lastUpdateTime", 3L);
        json.add("numberOfEvents", 4L);
        json.add("numberOfOtherOperations", 5L);
        json.add("getCount", 6L);
        json.add("putCount", 7L);
        json.add("removeCount", 8L);
        json.add("totalGetLatencies", 9L);
        json.add("totalPutLatencies", 10L);
        json.add("totalRemoveLatencies", 11L);
        json.add("maxGetLatency", 12L);
        json.add("maxPutLatency", 13L);
        json.add("maxRemoveLatency", 14L);

        json.add("ownedEntryCount", 15L);
        json.add("backupEntryCount", 16L);
        json.add("ownedEntryMemoryCost", 17L);
        json.add("backupEntryMemoryCost", 18L);
        json.add("heapCost", 19L);
        json.add("lockedEntryCount", 20L);
        json.add("dirtyEntryCount", 21L);
        json.add("backupCount", 22L);
        json.add("hits", 23L);

        final JsonObject nearCacheStatsJson = new JsonObject();
        nearCacheStatsJson.add("ownedEntryCount", 24L);
        nearCacheStatsJson.add("ownedEntryMemoryCost", 25L);
        nearCacheStatsJson.add("creationTime", 26L);
        nearCacheStatsJson.add("hits", 27L);
        nearCacheStatsJson.add("misses", 28L);
        json.add("nearCacheStats", nearCacheStatsJson);

        final LocalMapStatsImpl localMapStats = new LocalMapStatsImpl();
        localMapStats.fromJson(json);

        assertEquals(localMapStats.getCreationTime(), 1L);
        assertEquals(localMapStats.getLastAccessTime(), 2L);
        assertEquals(localMapStats.getLastUpdateTime(), 3L);
        assertEquals(localMapStats.getEventOperationCount(), 4L);
        assertEquals(localMapStats.getOtherOperationCount(), 5L);
        assertEquals(localMapStats.getGetOperationCount(), 6L);
        assertEquals(localMapStats.getPutOperationCount(), 7L);
        assertEquals(localMapStats.getRemoveOperationCount(), 8L);
        assertEquals(localMapStats.getTotalGetLatency(), 9L);
        assertEquals(localMapStats.getTotalPutLatency(), 10L);
        assertEquals(localMapStats.getTotalRemoveLatency(), 11L);
        assertEquals(localMapStats.getMaxGetLatency(), 12L);
        assertEquals(localMapStats.getMaxPutLatency(), 13L);
        assertEquals(localMapStats.getMaxRemoveLatency(), 14L);

        assertEquals(localMapStats.getOwnedEntryCount(), 15L);
        assertEquals(localMapStats.getBackupEntryCount(), 16L);
        assertEquals(localMapStats.getOwnedEntryMemoryCost(), 17L);
        assertEquals(localMapStats.getBackupEntryMemoryCost(), 18L);
        assertEquals(localMapStats.getHeapCost(), 19L);
        assertEquals(localMapStats.getLockedEntryCount(), 20L);
        assertEquals(localMapStats.getDirtyEntryCount(), 21L);
        assertEquals(localMapStats.getBackupCount(), 22L);
        assertEquals(localMapStats.getHits(), 23L);

        final NearCacheStats nearCacheStats = localMapStats.getNearCacheStats();
        assertEquals(nearCacheStats.getOwnedEntryCount(), 24L);
        assertEquals(nearCacheStats.getOwnedEntryMemoryCost(), 25L);
        assertEquals(nearCacheStats.getCreationTime(), 26L);
        assertEquals(nearCacheStats.getHits(), 27L);
        assertEquals(nearCacheStats.getMisses(), 28L);

    }

    @Test
    public void testLocalMapStats_withMemberGroups() throws Exception {
        final String mapName = randomMapName();
        final String[] firstMemberGroup = {"127.0.0.1", "127.0.0.2"};
        final String[] secondMemberGroup = {"127.0.0.3"};

        final Config config = createConfig(mapName, firstMemberGroup, secondMemberGroup);
        final String[] addressArray = concatenateArrays(firstMemberGroup, secondMemberGroup);

        final TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(addressArray);
        final HazelcastInstance node1 = factory.newHazelcastInstance(config);
        final HazelcastInstance node2 = factory.newHazelcastInstance(config);
        final HazelcastInstance node3 = factory.newHazelcastInstance(config);

        final IMap<Object, Object> test = node3.getMap(mapName);
        test.put(1, 1);

        assertBackupEntryCount(1, mapName, factory.getAllHazelcastInstances());
    }

    private void assertBackupEntryCount(final long expectedBackupEntryCount, final String mapName,
                                        final Collection<HazelcastInstance> nodes) {

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                long backup = 0;
                for (HazelcastInstance node : nodes) {
                    final IMap<Object, Object> map = node.getMap(mapName);
                    backup += getBackupEntryCount(map);
                }
                assertEquals(expectedBackupEntryCount, backup);
            }
        });
    }

    private long getBackupEntryCount(IMap<Object, Object> map) {
        final LocalMapStats localMapStats = map.getLocalMapStats();
        return localMapStats.getBackupEntryCount();
    }

    private String[] concatenateArrays(String[]... arrays) {
        int len = 0;
        for (String[] array : arrays) {
            len += array.length;
        }
        String[] result = new String[len];
        int destPos = 0;
        for (String[] array : arrays) {
            System.arraycopy(array, 0, result, destPos, array.length);
            destPos += array.length;
        }
        return result;
    }

    private Config createConfig(String mapName, String[] firstGroup, String[] secondGroup) {
        final MemberGroupConfig firstGroupConfig = createGroupConfig(firstGroup);
        final MemberGroupConfig secondGroupConfig = createGroupConfig(secondGroup);

        Config config = new Config();
        config.getPartitionGroupConfig().setEnabled(true)
                .setGroupType(PartitionGroupConfig.MemberGroupType.CUSTOM);
        config.getPartitionGroupConfig().addMemberGroupConfig(firstGroupConfig);
        config.getPartitionGroupConfig().addMemberGroupConfig(secondGroupConfig);
        config.getNetworkConfig().getInterfaces().addInterface("127.0.0.*");

        config.getMapConfig(mapName).setBackupCount(2);

        return config;
    }

    private MemberGroupConfig createGroupConfig(String[] addressArray) {
        final MemberGroupConfig memberGroupConfig = new MemberGroupConfig();
        for (String address : addressArray) {
            memberGroupConfig.addInterface(address);
        }
        return memberGroupConfig;
    }
}
