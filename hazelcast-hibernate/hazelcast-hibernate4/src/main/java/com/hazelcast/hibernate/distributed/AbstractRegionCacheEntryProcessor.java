package com.hazelcast.hibernate.distributed;

import com.hazelcast.hibernate.serialization.Expirable;
import com.hazelcast.hibernate.serialization.HibernateDataSerializerHook;
import com.hazelcast.map.EntryBackupProcessor;
import com.hazelcast.map.EntryProcessor;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.util.Map;

/**
 * An abstract implementation of {@link EntryProcessor} which acts on a hibernate region cache
 * {@link com.hazelcast.core.IMap}
 */
public abstract class AbstractRegionCacheEntryProcessor implements EntryProcessor<Object, Expirable>,
        EntryBackupProcessor<Object, Expirable>, IdentifiedDataSerializable {

    @Override
    public int getFactoryId() {
        return HibernateDataSerializerHook.F_ID;
    }

    @Override
    public void processBackup(Map.Entry<Object, Expirable> entry) {
        process(entry);
    }

    @Override
    public EntryBackupProcessor<Object, Expirable> getBackupProcessor() {
        return this;
    }

}
