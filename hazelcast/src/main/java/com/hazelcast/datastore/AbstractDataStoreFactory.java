package com.hazelcast.datastore;

import com.hazelcast.config.ExternalDataStoreConfig;
import com.hazelcast.spi.annotation.Beta;

/**
 * Basic implementation of the {@link ExternalDataStoreFactory}.
 * Requires implementing only how to create a datastore and how to close the factory
 *
 * @param <DS> - type of the data store
 * @since 5.2
 */
@Beta
public abstract class AbstractDataStoreFactory<DS> implements ExternalDataStoreFactory<DS> {

    protected DS sharedDataStore;
    protected ExternalDataStoreConfig config;

    /**
     * Create a new data store
     *
     * @return
     */
    abstract protected DS createDataSource();

    @Override
    public void init(ExternalDataStoreConfig config) {
        this.config = config;
        if (config.isShared()) {
            sharedDataStore = createDataSource();
        }
    }

    @Override
    public DataStoreSupplier<DS> createDataStore() {
        return config.isShared() ? DataStoreSupplier.nonClosing(sharedDataStore) : DataStoreSupplier.closing(createDataSource());
    }

}
