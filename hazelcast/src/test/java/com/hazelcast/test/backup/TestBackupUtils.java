package com.hazelcast.test.backup;

import com.hazelcast.cache.HazelcastCacheManager;
import com.hazelcast.cache.impl.CacheService;
import com.hazelcast.cache.impl.HazelcastServerCacheManager;
import com.hazelcast.cache.impl.HazelcastServerCachingProvider;
import com.hazelcast.cache.impl.ICacheRecordStore;
import com.hazelcast.cache.impl.record.CacheRecord;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.partition.InternalPartition;
import com.hazelcast.internal.partition.InternalPartitionService;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.PartitionContainer;
import com.hazelcast.map.impl.recordstore.RecordStore;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.partition.IPartition;
import com.hazelcast.spi.serialization.SerializationService;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestTaskExecutorUtil;

import javax.cache.spi.CachingProvider;
import java.util.Arrays;
import java.util.concurrent.Callable;

import static com.hazelcast.test.HazelcastTestSupport.assertTrueEventually;
import static com.hazelcast.test.HazelcastTestSupport.getNode;
import static com.hazelcast.test.HazelcastTestSupport.getNodeEngineImpl;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

/**
 * Convenience for accessing and asserting backup records.
 *
 */
public final class TestBackupUtils {
    private static final int DEFAULT_REPLICA_INDEX = 1;

    private TestBackupUtils() {

    }

    /**
     * Create a new instance of {@link BackupAccessor} for a given map.
     * It access a first backup replica.
     *
     * @param cluster all instances in the cluster
     * @param mapName map to access
     * @param <K> type of keys
     * @param <V> type of values
     * @return accessor for a given map and first backup replica
     */
    public static <K, V> BackupAccessor<K, V> newMapAccessor(HazelcastInstance[] cluster, String mapName) {
        return newMapAccessor(cluster, mapName, DEFAULT_REPLICA_INDEX);
    }

    /**
     * Create a new instance of {@link BackupAccessor} for a given map.
     * It allows to access an arbitrary replica index as long as it's greater or equals 1
     *
     * @param cluster all instances in the cluster
     * @param mapName map to access
     * @param replicaIndex replica index to access
     * @param <K> type of keys
     * @param <V> type of values
     * @return accessor for a given map and replica index
     */
    public static <K, V> BackupAccessor<K, V> newMapAccessor(HazelcastInstance[] cluster, String mapName, int replicaIndex) {
        return new MapBackupAccessor<K, V>(cluster, mapName, replicaIndex);
    }

    /**
     * Create a new instance of {@link BackupAccessor} for a given cache.
     * It access a first backup replica.
     *
     * @param cluster all instances in the cluster
     * @param cacheName cache to access
     * @param <K> type of keys
     * @param <V> type of values
     * @return accessor for a given cache and first backup replica
     */
    public static <K, V> BackupAccessor<K, V> newCacheAccessor(HazelcastInstance[] cluster, String cacheName) {
        return newCacheAccessor(cluster, cacheName, DEFAULT_REPLICA_INDEX);
    }

    /**
     * Create a new instance of {@link BackupAccessor} for a given cache.
     * It allows to access an arbitrary replica index as long as it's greater or equals 1
     *
     * @param cluster all instances in the cluster
     * @param cacheName cache to access
     * @param replicaIndex replica index to access
     * @param <K> type of keys
     * @param <V> type of values
     * @return accessor for a given cache and replica index
     */
    public static <K, V> BackupAccessor<K, V> newCacheAccessor(HazelcastInstance[] cluster, String cacheName, int replicaIndex) {
        return new CacheBackupAccessor<K, V>(cluster, cacheName, replicaIndex);
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


    // ######################################################################
    // ##### PRIVATE STUFF BELLOW, NO NEED TO TOUCH IT IN REGULAR TESTS #####
    // ######################################################################

    private static class CacheBackupAccessor<K, V> extends BackupAccessorSupport<K, V> implements BackupAccessor<K ,V> {
        private final String cacheName;
        private final int replicaIndex;

        private CacheBackupAccessor(HazelcastInstance[] cluster, String cacheName, int replicaIndex) {
            super(cluster);
            if (replicaIndex < 1 || replicaIndex > IPartition.MAX_BACKUP_COUNT) {
                throw new IllegalArgumentException("Cannot access replica index " + replicaIndex);
            }

            if (cluster == null || cluster.length == 0) {
                throw new IllegalArgumentException("Cluster has to have at least 1 member.");
            }

            this.cacheName = cacheName;
            this.replicaIndex = replicaIndex;
        }


        @Override
        public int size() {
            InternalPartitionService partitionService = getNodeEngineImpl(cluster[0]).getPartitionService();
            IPartition[] partitions = partitionService.getPartitions();
            int count = 0;
            for (final IPartition partition : partitions) {
                Address replicaAddress = partition.getReplicaAddress(replicaIndex);
                if (replicaAddress == null) {
                    continue;
                }
                HazelcastInstance instance = getInstanceWithAddress(replicaAddress);
                NodeEngineImpl nodeEngineImpl = HazelcastTestSupport.getNodeEngineImpl(instance);
                final CacheService service = nodeEngineImpl.getService(CacheService.SERVICE_NAME);

                CachingProvider provider = HazelcastServerCachingProvider.createCachingProvider(instance);
                HazelcastCacheManager cacheManager = (HazelcastServerCacheManager) provider.getCacheManager();

                final String cacheNameWithPrefix = cacheManager.getCacheNameWithPrefix(cacheName);
                count += TestTaskExecutorUtil.runOnPartitionThread(instance, new Callable<Integer>() {
                    @Override
                    public Integer call() {
                        ICacheRecordStore recordStore = service.getRecordStore(cacheNameWithPrefix, partition.getPartitionId());
                        if (recordStore == null) {
                            return 0;
                        }
                        return recordStore.size();
                    }
                }, partition.getPartitionId());
            }
            return count;
        }

        @Override
        public V get(final K key) {
            final InternalPartition partition = getPartitionForKey(key);
            Address replicaAddress = partition.getReplicaAddress(replicaIndex);
            if (replicaAddress == null) {
                // there is no owner of this replica (yet?)
                return null;
            }

            HazelcastInstance hz = getInstanceWithAddress(replicaAddress);
            if (hz == null) {
                throw new IllegalStateException("Partition " + partition + " with replica index " + replicaIndex
                        + " is mapped to " + replicaAddress + " but there is no member with this address in the cluster."
                        + " List of known members: " + Arrays.toString(cluster));
            }
            CachingProvider provider = HazelcastServerCachingProvider.createCachingProvider(hz);
            HazelcastCacheManager cacheManager = (HazelcastServerCacheManager) provider.getCacheManager();
            final String cacheNameWithPrefix = cacheManager.getCacheNameWithPrefix(cacheName);
            NodeEngineImpl nodeEngine = getNodeEngineImpl(hz);
            final CacheService cacheService = nodeEngine.getService(CacheService.SERVICE_NAME);
            final SerializationService serializationService = nodeEngine.getSerializationService();
            return TestTaskExecutorUtil.runOnPartitionThread(hz, new Callable<V>() {
                @Override
                public V call() {
                    ICacheRecordStore recordStore = cacheService.getRecordStore(cacheNameWithPrefix, partition.getPartitionId());
                    if (recordStore == null) {
                        return null;
                    }
                    Data keyData = serializationService.toData(key);
                    CacheRecord cacheRecord = recordStore.getReadOnlyRecords().get(keyData);
                    if (cacheRecord == null) {
                        return null;
                    }
                    Object value = cacheRecord.getValue();
                    return serializationService.toObject(value);
                }
            }, partition.getPartitionId());
        }
    }

    private static class MapBackupAccessor<K, V> extends BackupAccessorSupport<K, V> implements BackupAccessor<K, V> {
        private final String mapName;
        private final int replicaIndex;

        private MapBackupAccessor(HazelcastInstance[] cluster, String mapName, int replicaIndex) {
            super(cluster);
            if (replicaIndex < 1 || replicaIndex > IPartition.MAX_BACKUP_COUNT) {
                throw new IllegalArgumentException("Cannot access replica index " + replicaIndex);
            }

            if (cluster == null || cluster.length == 0) {
                throw new IllegalArgumentException("Cluster has to have at least 1 member.");
            }

            this.mapName = mapName;
            this.replicaIndex = replicaIndex;
        }

        public int size() {
            InternalPartitionService partitionService = getNodeEngineImpl(cluster[0]).getPartitionService();
            IPartition[] partitions = partitionService.getPartitions();
            int count = 0;
            for (IPartition partition : partitions) {
                Address replicaAddress = partition.getReplicaAddress(replicaIndex);
                if (replicaAddress == null) {
                    continue;
                }
                HazelcastInstance instance = getInstanceWithAddress(replicaAddress);
                NodeEngineImpl nodeEngineImpl = HazelcastTestSupport.getNodeEngineImpl(instance);
                MapService service = nodeEngineImpl.getService(MapService.SERVICE_NAME);

                MapServiceContext context = service.getMapServiceContext();
                final PartitionContainer container = context.getPartitionContainer(partition.getPartitionId());

                count += TestTaskExecutorUtil.runOnPartitionThread(instance, new Callable<Integer>() {
                    @Override
                    public Integer call() {
                        RecordStore recordStore = container.getExistingRecordStore(mapName);
                        if (recordStore == null) {
                            return 0;
                        }
                        return recordStore.size();
                    }
                }, partition.getPartitionId());
            }
            return count;
        }


        public V get(final K key) {
            final InternalPartition partition = getPartitionForKey(key);
            Address replicaAddress = partition.getReplicaAddress(replicaIndex);
            if (replicaAddress == null) {
                // there is no owner of this replica (yet?)
                return null;
            }

            HazelcastInstance hz = getInstanceWithAddress(replicaAddress);
            if (hz == null) {
                throw new IllegalStateException("Partition " + partition + " with replica index " + replicaIndex
                        + " is mapped to " + replicaAddress + " but there is no member with this address in the cluster."
                        + " List of known members: " + Arrays.toString(cluster));
            }

            final SerializationService serializationService = getNodeEngineImpl(hz).getSerializationService();
            MapService mapService = HazelcastTestSupport.getNodeEngineImpl(hz).getService(MapService.SERVICE_NAME);
            final MapServiceContext context = mapService.getMapServiceContext();

            return TestTaskExecutorUtil.runOnPartitionThread(hz, new Callable<V>() {
                @Override
                public V call() {
                    PartitionContainer partitionContainer = context.getPartitionContainer(partition.getPartitionId());
                    RecordStore recordStore = partitionContainer.getExistingRecordStore(mapName);
                    if (recordStore == null) {
                        return null;
                    }
                    Data keyData = serializationService.toData(key);
                    Object o = recordStore.get(keyData, true);
                    if (o == null) {
                        return null;
                    }
                    return serializationService.toObject(o);
                }
            }, partition.getPartitionId());
        }
    }

    private abstract static class BackupAccessorSupport<K, V> implements BackupAccessor<K, V> {
        protected final HazelcastInstance[] cluster;

        protected BackupAccessorSupport(HazelcastInstance[] cluster) {
            this.cluster = cluster;
        }

        protected InternalPartition getPartitionForKey(K key) {
            //to determine partition we can use any instance. let's pick the 1st one
            HazelcastInstance instance = cluster[0];
            InternalPartitionService partitionService = getNode(instance).getPartitionService();
            int partitionId = partitionService.getPartitionId(key);
            InternalPartition partition = partitionService.getPartition(partitionId);
            return partition;
        }

        protected HazelcastInstance getInstanceWithAddress(Address address) {
            for (HazelcastInstance hz : cluster) {
                if (hz.getCluster().getLocalMember().getAddress().equals(address)) {
                    return hz;
                }
            }
            throw new IllegalStateException("Address " + address + " not found in the cluster");
        }
    }
}
