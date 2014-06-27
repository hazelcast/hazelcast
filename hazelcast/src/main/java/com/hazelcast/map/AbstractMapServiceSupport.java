package com.hazelcast.map;

import com.hazelcast.core.PartitioningStrategy;
import com.hazelcast.logging.ILogger;
import com.hazelcast.monitor.impl.LocalMapStatsImpl;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.SerializationService;
import com.hazelcast.partition.InternalPartitionService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.util.Clock;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Contains common functionality of map service support classes.
 */
abstract class AbstractMapServiceSupport {

    protected final AtomicReference<List<Integer>> ownedPartitions;
    protected final PartitionContainer[] partitionContainers;
    protected final NodeEngine nodeEngine;
    protected final ILogger logger;
    protected final ConcurrentMap<String, MapContainer> mapContainers;

    /**
     * Per node global write behind queue item counter.
     * Creating here because we want to have a counter per node.
     * This is used by owner and backups together so it should be defined
     * getting this into account.
     */
    private final AtomicInteger writeBehindQueueItemCounter = new AtomicInteger(0);

    protected AbstractMapServiceSupport(NodeEngine nodeEngine) {
        this.nodeEngine = nodeEngine;
        this.mapContainers = new ConcurrentHashMap<String, MapContainer>();
        this.partitionContainers = new PartitionContainer[nodeEngine.getPartitionService().getPartitionCount()];
        this.logger = nodeEngine.getLogger(MapService.class);
        this.ownedPartitions = new AtomicReference<List<Integer>>();
    }

    public SerializationService getSerializationService() {
        return nodeEngine.getSerializationService();
    }

    public PartitionContainer getPartitionContainer(int partitionId) {
        return partitionContainers[partitionId];
    }

    public RecordStore getRecordStore(int partitionId, String mapName) {
        return getPartitionContainer(partitionId).getRecordStore(mapName);
    }

    public RecordStore getExistingRecordStore(int partitionId, String mapName) {
        return getPartitionContainer(partitionId).getExistingRecordStore(mapName);
    }

    public List<Integer> getOwnedPartitions() {
        List<Integer> partitions = ownedPartitions.get();
        if (partitions == null) {
            partitions = getMemberPartitions();
            ownedPartitions.set(partitions);
        }
        return partitions;
    }

    protected String getServiceName() {
        return MapService.SERVICE_NAME;
    }

    abstract MapContainer getMapContainer(String mapName);

    abstract LocalMapStatsImpl getLocalMapStatsImpl(String name);

    protected abstract MapService getService();

    public AtomicInteger getWriteBehindQueueItemCounter() {
        return writeBehindQueueItemCounter;
    }

    protected void clearPartitionData(final int partitionId) {
        final PartitionContainer container = partitionContainers[partitionId];
        if (container != null) {
            for (RecordStore mapPartition : container.getMaps().values()) {
                mapPartition.clearPartition();
            }
            container.getMaps().clear();
        }
    }

    protected List<Integer> getMemberPartitions() {
        InternalPartitionService partitionService = nodeEngine.getPartitionService();
        List<Integer> partitions = partitionService.getMemberPartitions(nodeEngine.getThisAddress());
        return Collections.unmodifiableList(partitions);
    }

    public NodeEngine getNodeEngine() {
        return nodeEngine;
    }

    public static long getNow() {
        return Clock.currentTimeMillis();
    }

    public static long convertTime(long seconds, TimeUnit unit) {
        return unit.toMillis(seconds);
    }

    public Object toObject(Object data) {
        if (data == null) {
            return null;
        }
        if (data instanceof Data) {
            return nodeEngine.toObject(data);
        } else {
            return data;
        }
    }

    public Data toData(Object object, PartitioningStrategy partitionStrategy) {
        if (object == null) {
            return null;
        }
        if (object instanceof Data) {
            return (Data) object;
        } else {
            return nodeEngine.getSerializationService().toData(object, partitionStrategy);
        }
    }

    public Data toData(Object object) {
        if (object == null) {
            return null;
        }
        if (object instanceof Data) {
            return (Data) object;
        } else {
            return nodeEngine.getSerializationService().toData(object);
        }
    }

    public boolean compare(String mapName, Object value1, Object value2) {
        if (value1 == null && value2 == null) {
            return true;
        }
        if (value1 == null) {
            return false;
        }
        if (value2 == null) {
            return false;
        }
        final MapContainer mapContainer = getMapContainer(mapName);
        return mapContainer.getRecordFactory().isEquals(value1, value2);
    }

    protected long getTimeToLive(MapContainer mapContainer) {
        return MapService.convertTime(mapContainer.getMapConfig().getTimeToLiveSeconds(), TimeUnit.SECONDS);
    }

    public ILogger getLogger() {
        return logger;
    }
}
