package com.hazelcast.config;

import java.util.ArrayList;
import java.util.List;

/**
 * @ali 9/12/13
 */
public abstract class CollectionConfig<T extends CollectionConfig> {

    public final static int DEFAULT_MAX_SIZE = 0;
    public final static int DEFAULT_SYNC_BACKUP_COUNT = 1;
    public final static int DEFAULT_ASYNC_BACKUP_COUNT = 0;

    private String name;
    private List<ItemListenerConfig> listenerConfigs;
    private int backupCount = DEFAULT_SYNC_BACKUP_COUNT;
    private int asyncBackupCount = DEFAULT_ASYNC_BACKUP_COUNT;
    private int maxSize = DEFAULT_MAX_SIZE;
    private boolean statisticsEnabled = true;

    protected CollectionConfig() {
    }

    protected CollectionConfig(CollectionConfig config) {
        this.name = config.name;
        this.listenerConfigs = config.listenerConfigs;
        this.backupCount = config.backupCount;
        this.asyncBackupCount = config.asyncBackupCount;
        this.maxSize = config.maxSize;
        this.statisticsEnabled = config.statisticsEnabled;
    }

    public String getName() {
        return name;
    }

    public T setName(String name) {
        this.name = name;
        return (T)this;
    }

    public List<ItemListenerConfig> getListenerConfigs() {
        if (listenerConfigs == null){
            listenerConfigs = new ArrayList<ItemListenerConfig>();
        }
        return listenerConfigs;
    }

    public T setListenerConfigs(List<ItemListenerConfig> listenerConfigs) {
        this.listenerConfigs = listenerConfigs;
        return (T)this;
    }

    public int getTotalBackupCount() {
        return backupCount + asyncBackupCount;
    }

    public int getBackupCount() {
        return backupCount;
    }

    public T setBackupCount(int backupCount) {
        this.backupCount = backupCount;
        return (T)this;
    }

    public int getAsyncBackupCount() {
        return asyncBackupCount;
    }

    public T setAsyncBackupCount(int asyncBackupCount) {
        this.asyncBackupCount = asyncBackupCount;
        return (T)this;
    }

    public int getMaxSize() {
        return maxSize;
    }

    public T setMaxSize(int maxSize) {
        this.maxSize = maxSize;
        return (T)this;
    }

    public boolean isStatisticsEnabled() {
        return statisticsEnabled;
    }

    public T setStatisticsEnabled(boolean statisticsEnabled) {
        this.statisticsEnabled = statisticsEnabled;
        return (T)this;
    }

    public void addItemListenerConfig(ItemListenerConfig itemListenerConfig){
        getListenerConfigs().add(itemListenerConfig);
    }
}
