package com.hazelcast.map.mapstore.writebehind;

import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MapStoreConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.core.MapStore;
import com.hazelcast.instance.GroupProperties;
import com.hazelcast.test.TestHazelcastInstanceFactory;

import static com.hazelcast.test.HazelcastTestSupport.randomMapName;

public class TestMapUsingMapStoreBuilder<K, V> {

    private HazelcastInstance[] nodes;

    private int nodeCount;

    private int partitionCount = 271;

    private int backupCount;

    private String mapName = randomMapName("default");

    private MapStore<K, V> mapStore;

    private int writeDelaySeconds = 0;

    private int writeBatchSize = 1;

    private InMemoryFormat inMemoryFormat = InMemoryFormat.BINARY;

    private int backupDelaySeconds = 10;

    private TestHazelcastInstanceFactory instanceFactory;

    private TestMapUsingMapStoreBuilder() {
    }

    public static <K, V> TestMapUsingMapStoreBuilder<K, V> create() {
        return new TestMapUsingMapStoreBuilder<K, V>();
    }


    public TestMapUsingMapStoreBuilder<K, V> mapName(String mapName) {
        if (mapName == null) {
            throw new IllegalArgumentException("mapName is null");
        }
        this.mapName = mapName;
        return this;
    }

    public TestMapUsingMapStoreBuilder<K, V> withNodeCount(int nodeCount) {
        if (nodeCount < 1) {
            throw new IllegalArgumentException("nodeCount < 1");
        }
        this.nodeCount = nodeCount;
        return this;
    }

    public TestMapUsingMapStoreBuilder<K, V> withNodeFactory(TestHazelcastInstanceFactory factory) {
        this.instanceFactory = factory;
        return this;
    }

    public TestMapUsingMapStoreBuilder<K, V> withBackupProcessingDelay(int backupDelaySeconds) {
        if (backupDelaySeconds < 0) {
            throw new IllegalArgumentException("delaySeconds < 0");
        }
        this.backupDelaySeconds = backupDelaySeconds;
        return this;
    }

    public TestMapUsingMapStoreBuilder<K, V> withPartitionCount(int partitionCount) {
        if (partitionCount < 1) {
            throw new IllegalArgumentException("partitionCount < 1");
        }
        this.partitionCount = partitionCount;
        return this;
    }

    public TestMapUsingMapStoreBuilder<K, V> withBackupCount(int backupCount) {
        if (backupCount < 0) {
            throw new IllegalArgumentException("backupCount < 1 but found [" + backupCount + ']');
        }
        this.backupCount = backupCount;
        return this;
    }


    public TestMapUsingMapStoreBuilder<K, V> withMapStore(MapStore<K, V> mapStore) {
        this.mapStore = mapStore;
        return this;
    }

    public TestMapUsingMapStoreBuilder<K, V> withWriteDelaySeconds(int writeDelaySeconds) {
        if (writeDelaySeconds < 0) {
            throw new IllegalArgumentException("writeDelaySeconds < 0");
        }
        this.writeDelaySeconds = writeDelaySeconds;
        return this;
    }

    public TestMapUsingMapStoreBuilder<K, V> withInMemoryFormat(InMemoryFormat inMemoryFormat) {
        this.inMemoryFormat = inMemoryFormat;
        return this;
    }


    public TestMapUsingMapStoreBuilder<K, V> withWriteBatchSize(int writeBatchSize) {
        this.writeBatchSize = writeBatchSize;
        return this;
    }


    public IMap<K, V> build() {
        if (backupCount != 0 && backupCount > nodeCount - 1) {
            throw new IllegalArgumentException("backupCount > nodeCount - 1");
        }
        final MapStoreConfig mapStoreConfig = new MapStoreConfig();
        mapStoreConfig
                .setImplementation(mapStore)
                .setWriteDelaySeconds(writeDelaySeconds)
                .setWriteBatchSize(writeBatchSize);

        final Config config = new Config();
        config.getMapConfig(mapName)
                .setBackupCount(backupCount)
                .setMapStoreConfig(mapStoreConfig).setInMemoryFormat(inMemoryFormat);

        config.setProperty(GroupProperties.PROP_PARTITION_COUNT, String.valueOf(partitionCount));
        if (backupDelaySeconds > 0) {
            config.setProperty(GroupProperties.PROP_MAP_REPLICA_SCHEDULED_TASK_DELAY_SECONDS,
                    String.valueOf(backupCount));
        }
        // nodes.
        nodes = new HazelcastInstance[nodeCount];
        for(int i = 0; i < nodeCount; i++) {
            nodes[i] = instanceFactory.newHazelcastInstance(config);
        }
        return nodes[0].getMap(mapName);
    }

    public HazelcastInstance[] getNodes() {
        return nodes;
    }
}
