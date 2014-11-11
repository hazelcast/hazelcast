package com.hazelcast.cache.impl.operation;

import com.hazelcast.cache.impl.CacheDataSerializerHook;
import com.hazelcast.cache.impl.CacheService;
import com.hazelcast.cache.impl.ICacheRecordStore;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.BackupOperation;
import com.hazelcast.spi.impl.AbstractNamedOperation;

/**
 * Backup operation of {@link com.hazelcast.cache.impl.operation.CacheClearOperation}.
 * <p>It simply clears the records.</p>
 */
public class CacheClearBackupOperation extends AbstractNamedOperation
        implements BackupOperation, IdentifiedDataSerializable {

    private transient ICacheRecordStore cache;

    public CacheClearBackupOperation() {
    }

    public CacheClearBackupOperation(String name) {
        super(name);
    }

    @Override
    public void beforeRun()
            throws Exception {
        CacheService service = getService();
        cache = service.getOrCreateCache(name, getPartitionId());
    }

    @Override
    public void run() throws Exception {
        cache.clear();
    }

    @Override
    public int getFactoryId() {
        return CacheDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return CacheDataSerializerHook.CLEAR_BACKUP;
    }

}
