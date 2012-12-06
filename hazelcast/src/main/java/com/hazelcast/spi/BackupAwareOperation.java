package com.hazelcast.spi;

/**
 * @mdogan 12/6/12
 */
public interface BackupAwareOperation {

    int getSyncBackupCount();

    int getAsyncBackupCount();

    Operation getBackupOperation();

}
