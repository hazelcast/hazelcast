package com.hazelcast.cache.impl.operation;

import com.hazelcast.cache.impl.CacheClearResponse;
import com.hazelcast.cache.impl.CacheDataSerializerHook;
import com.hazelcast.cache.impl.ICacheRecordStore;
import com.hazelcast.cache.impl.ICacheService;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.HeapData;
import com.hazelcast.partition.InternalPartitionService;
import com.hazelcast.spi.BackupAwareOperation;
import com.hazelcast.spi.Operation;

import javax.cache.CacheException;
import java.util.HashSet;
import java.util.Set;

/**
 * TODO add a proper JavaDoc
 */
public class CacheRemoveAllOperation extends PartitionWideCacheOperation implements BackupAwareOperation {

    private Set<Data> keys;

    private int completionId;

    private transient Set<Data> filteredKeys = new HashSet<Data>();

    private transient ICacheRecordStore cache;

    public CacheRemoveAllOperation() {
    }

    public CacheRemoveAllOperation(String name, Set<Data> keys, int completionId) {
        super(name);
        this.keys = keys;
        this.completionId = completionId;
    }

    @Override
    public boolean shouldBackup() {
        return !filteredKeys.isEmpty();
    }

    @Override
    public final int getSyncBackupCount() {
        return cache != null ? cache.getConfig().getBackupCount() : 0;
    }

    @Override
    public final int getAsyncBackupCount() {
        return cache != null ? cache.getConfig().getAsyncBackupCount() : 0;
    }

    @Override
    public Operation getBackupOperation() {
        return new CacheRemoveAllBackupOperation(name, filteredKeys);
    }

    @Override
    public int getId() {
        return CacheDataSerializerHook.REMOVE_ALL;
    }

    @Override
    public void beforeRun() throws Exception {
        ICacheService service = getService();
        cache = service.getCacheRecordStore(name, getPartitionId());
    }

    @Override
    public void run() throws Exception {
        if (cache == null) {
            return;
        }
        filterKeys();
        try {
            if (keys == null) {
                // Here the filteredKeys is empty, this means we will remove all data
                // filteredKeys will get filled with removed keys
                cache.removeAll(filteredKeys);
            } else if (!filteredKeys.isEmpty()) {
                cache.removeAll(filteredKeys);
            }
            response = new CacheClearResponse(Boolean.TRUE);
            int orderKey = keys != null ? keys.hashCode() : 1;
            cache.publishCompletedEvent(name, completionId, new HeapData(), orderKey);
        } catch (CacheException e) {
            response = new CacheClearResponse(e);
        }
    }

    private void filterKeys() {
        if (keys == null) {
            return;
        }
        InternalPartitionService partitionService = getNodeEngine().getPartitionService();
        for (Data k : keys) {
            if (partitionService.getPartitionId(k) == getPartitionId()) {
                filteredKeys.add(k);
            }
        }
    }

}
