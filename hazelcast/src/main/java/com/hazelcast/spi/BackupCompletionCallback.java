package com.hazelcast.spi;

public interface BackupCompletionCallback {

    void signalOneBackupComplete();
}
