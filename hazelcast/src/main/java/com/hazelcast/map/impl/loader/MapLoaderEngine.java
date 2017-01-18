package com.hazelcast.map.impl.loader;

import com.hazelcast.logging.ILogger;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.mapstore.MapStoreContext;
import com.hazelcast.map.impl.recordstore.RecordStoreLoader;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.exception.RetryableHazelcastException;
import com.hazelcast.util.ExceptionUtil;
import com.hazelcast.util.FutureUtil;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Future;

public class MapLoaderEngine {

    protected final ILogger logger;

    protected final String name;
    protected final RecordStoreLoader recordStoreLoader;
    protected final KeyLoader keyLoader;

    // loadingFutures are modified by partition threads and could be accessed by query threads
    protected final Collection<Future> loadingFutures = new ConcurrentLinkedQueue<Future>();
    // record store may be created with or without triggering the load
    // this flag guards that the loading on create is invoked not more than once should the record store be migrated.
    private boolean loadedOnCreate;
    // records if the record store has been loaded just before the migrations starts
    // if so, the loading should NOT be started after the migration commit
    private boolean loadedOnPreMigration;

    private MapStoreContext mapStoreContext;
    private MapServiceContext mapServiceContext;
    private int partitionId;

    public MapLoaderEngine(String mapName, RecordStoreLoader recordStoreLoader, KeyLoader keyLoader, ILogger logger,
                           MapStoreContext mapStoreContext, MapServiceContext mapServiceContext, int partitionId) {
        this.name = mapName;
        this.logger = logger;
        this.keyLoader = keyLoader;
        this.recordStoreLoader = recordStoreLoader;
        this.loadedOnCreate = false;
        this.mapStoreContext = mapStoreContext;
        this.mapServiceContext = mapServiceContext;
        this.partitionId = partitionId;
    }

    public void startLoading() {
        if (logger.isFinestEnabled()) {
            logger.finest("StartLoading invoked " + getStateMessage());
        }
        if (mapStoreContext.isMapLoader() && !loadedOnCreate) {
            if (!loadedOnPreMigration) {
                if (logger.isFinestEnabled()) {
                    logger.finest("Triggering load " + getStateMessage());
                }
                loadedOnCreate = true;
                loadingFutures.add(keyLoader.startInitialLoad(mapStoreContext, partitionId));
            } else {
                if (logger.isFinestEnabled()) {
                    logger.finest("Promoting to loaded on migration " + getStateMessage());
                }
                keyLoader.promoteToLoadedOnMigration();
            }
        }
    }


    public void loadAll(boolean replaceExistingValues) {
        if (logger.isFinestEnabled()) {
            logger.finest("loadAll invoked " + getStateMessage());
        }

        logger.info("Starting to load all keys for map " + name + " on partitionId=" + partitionId);
        Future<?> loadingKeysFuture = keyLoader.startLoading(mapStoreContext, replaceExistingValues);
        loadingFutures.add(loadingKeysFuture);
    }


    public void loadAllFromStore(List<Data> keys, boolean replaceExistingValues) {
        if (!keys.isEmpty()) {
            Future f = recordStoreLoader.loadValues(keys, replaceExistingValues);
            loadingFutures.add(f);
        }

        // We should not track key loading here. IT's not key loading but values loading.
        // Apart from that it's irrelevant for RECEIVER nodes. SENDER and SENDER_BACKUP will track the key-loading anyway.
        // Fixes https://github.com/hazelcast/hazelcast/issues/9255
    }


    /**
     * Informs this recordStore about the loading status of the recordStore that this store is migrated from.
     * If the 'predecessor' has been loaded this record store should not trigger the load again.
     * Will be taken into account only if invoked before the startLoading method. Otherwise has no effect.
     * <p>
     * This method should be deleted when the map's lifecycle has been cleaned-up. Currently it's impossible to
     * pass additional state when the record store is created, thus this this state has to be passed in post-creation
     * setters which is cumbersome and error-prone.
     */
    public void setPreMigrationLoadedStatus(boolean loaded) {
        loadedOnPreMigration = loaded;
    }


    public boolean isLoaded() {
        return FutureUtil.allDone(loadingFutures);
    }


    public void updateLoadStatus(boolean lastBatch, Throwable exception) {
        keyLoader.trackLoading(lastBatch, exception);

        if (lastBatch) {
            logger.finest("Completed loading map " + name + " on partitionId=" + partitionId);
        }
    }


    public void maybeDoInitialLoad() {
        if (keyLoader.shouldDoInitialLoad()) {
            loadAll(false);
        }
    }


    public boolean isKeyLoadFinished() {
        return keyLoader.isKeyLoadFinished();
    }


    public void checkIfLoaded() {
        if (loadingFutures.isEmpty()) {
            return;
        }

        if (isLoaded()) {
            List<Future> doneFutures = null;
            try {
                doneFutures = FutureUtil.getAllDone(loadingFutures);
                // check all finished loading futures for exceptions
                FutureUtil.checkAllDone(doneFutures);
            } catch (Exception e) {
                logger.severe("Exception while loading map " + name, e);
                ExceptionUtil.rethrow(e);
            } finally {
                loadingFutures.removeAll(doneFutures);
            }
        } else {
            keyLoader.triggerLoadingWithDelay();
            throw new RetryableHazelcastException("Map " + name
                    + " is still loading data from external store");
        }
    }

    private String getStateMessage() {
        return "on partitionId=" + partitionId + " on " + mapServiceContext.getNodeEngine().getThisAddress()
                + " loadedOnCreate=" + loadedOnCreate + " loadedOnPreMigration=" + loadedOnPreMigration
                + " isLoaded=" + isLoaded();
    }


}
