package com.hazelcast.map;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.Node;
import com.hazelcast.internal.partition.InternalPartition;
import com.hazelcast.internal.partition.InternalPartitionService;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.PartitionContainer;
import com.hazelcast.map.impl.recordstore.RecordStore;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.partition.IPartition;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastTestSupport;

import static com.hazelcast.test.HazelcastTestSupport.assertTrueEventually;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

/**
 * Convenience for accessing backup records.
 *
 */
public final class TestBackupUtils {
    private static final int DEFAULT_REPLICA_INDEX = 1;

    private TestBackupUtils() {

    }

    /**
     * Access backup records in a given replica. Use {@link #newMapAccessor(HazelcastInstance[], String)}
     * to create an instance for your data structure.
     *
     * @param <K>
     * @param <V>
     */
    public interface BackupAccessor<K ,V> {
        /**
         * Number of backup entries
         *
         * @return
         */
        int size();

        /**
         * Reads backup value
         *
         * @param key
         * @return backup value or null
         */
        V get(K key);
    }

    /**
     * Create a new instance of {@link BackupAccessor} for a give map. It uses a first backup replica.
     *
     * @param cluster
     * @param mapName
     * @param <K>
     * @param <V>
     * @return
     */
    public static <K, V> BackupAccessor<K, V> newMapAccessor(HazelcastInstance[] cluster, String mapName) {
        return newMapAccessor(cluster, mapName, DEFAULT_REPLICA_INDEX);
    }

    public static <K, V> BackupAccessor<K, V> newMapAccessor(HazelcastInstance[] cluster, String mapName, int replicaIndex) {
        return new MapBackupAccessor<K, V>(cluster, mapName, replicaIndex);
    }

    public static <K, V> void assertBackupEntryEqualsEventually(final K key, final V expectedValue, final BackupAccessor<K, V> accessor) {
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                V actualValue = accessor.get(key);
                assertEquals("Backup entry with key '" + key + "' was '" + actualValue + "', but it was expected to be '"
                        + expectedValue + "'.", expectedValue, actualValue);
            }
        });
    }

    public static <K, V> void assertBackupEntryNullEventually(final K key, final BackupAccessor<K, V> accessor) {
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                V actualValue = accessor.get(key);
                assertNull("Backup entry with key '" + key + "' was '" + actualValue + "', but it was expected to be NULL.", actualValue);
            }
        });
    }

    public static <K, V> void assertBackupSizeEventually(final int expectedSize, final BackupAccessor<K, V> accessor) {
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                int actualSize = accessor.size();
                assertEquals("Expected size: ' " + expectedSize + "' actual size: " + actualSize, expectedSize, actualSize);
            }
        });
    }

    private static class MapBackupAccessor<K, V> implements BackupAccessor<K, V> {
        private final HazelcastInstance[] cluster;
        private final String mapName;
        private final int replicaIndex;
        private final InternalSerializationService serializationService;
        private final InternalPartitionService partitionService;

        private MapBackupAccessor(HazelcastInstance[] cluster, String mapName, int replicaIndex) {
            if (replicaIndex < 1) {
                throw new IllegalArgumentException("Cannot access replica index " + replicaIndex);
            }

            if (cluster == null || cluster.length == 0) {
                throw new IllegalArgumentException("Cluster has to have at least 1 member.");
            }

            this.cluster = cluster;
            this.mapName = mapName;
            this.replicaIndex = replicaIndex;

            Node node = HazelcastTestSupport.getNode(cluster[0]);
            partitionService = node.getPartitionService();
            serializationService = node.getSerializationService();
        }

        public int size() {
            IPartition[] partitions = partitionService.getPartitions();
            int count = 0;
            for (IPartition partition : partitions) {
                Address replicaAddress = partition.getReplicaAddress(replicaIndex);
                if (replicaAddress == null) {
                    continue;
                }
                HazelcastInstance instance = getInstancePerAddress(replicaAddress);

                NodeEngineImpl nodeEngineImpl = HazelcastTestSupport.getNodeEngineImpl(instance);
                MapService service = nodeEngineImpl.getService(MapService.SERVICE_NAME);

                MapServiceContext context = service.getMapServiceContext();
                PartitionContainer container = context.getPartitionContainer(partition.getPartitionId());

                RecordStore recordStore = container.getExistingRecordStore(mapName);
                if (recordStore == null) {
                    continue;
                }
                count += recordStore.size();
            }
            return count;
        }

        public V get(K key) {
            Data keyData = serializationService.toData(key);
            int partitionId = partitionService.getPartitionId(key);
            InternalPartition partition = partitionService.getPartition(partitionId);
            Address replicaAddress = partition.getReplicaAddress(replicaIndex);

            for (HazelcastInstance hz : cluster) {
                if (!hz.getCluster().getLocalMember().getAddress().equals(replicaAddress)) {
                    continue;
                }
                MapService mapService = HazelcastTestSupport.getNodeEngineImpl(hz).getService(MapService.SERVICE_NAME);
                MapServiceContext context = mapService.getMapServiceContext();

                PartitionContainer partitionContainer = context.getPartitionContainer(partitionId);
                RecordStore recordStore = partitionContainer.getExistingRecordStore(mapName);
                if (recordStore == null) {
                    return null;
                }
                Object o = recordStore.get(keyData, true);
                if (o == null) {
                    return null;
                }
                return serializationService.toObject(o);
            }
            return null;
        }

        private HazelcastInstance getInstancePerAddress(Address address) {
            for (HazelcastInstance hz : cluster) {
                if (hz.getCluster().getLocalMember().getAddress().equals(address)) {
                    return hz;
                }
            }
            throw new IllegalStateException("Address " + address + " not found in the cluster");
        }
    }
}
