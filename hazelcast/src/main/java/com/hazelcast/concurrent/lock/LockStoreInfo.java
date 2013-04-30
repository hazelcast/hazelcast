package com.hazelcast.concurrent.lock;

import com.hazelcast.spi.ObjectNamespace;

/**
 * @mdogan 4/30/13
 */
public interface LockStoreInfo {

    ObjectNamespace getObjectNamespace();

    int getBackupCount();

    int getAsyncBackupCount();
}
