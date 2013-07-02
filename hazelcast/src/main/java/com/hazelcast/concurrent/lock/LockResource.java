package com.hazelcast.concurrent.lock;

import com.hazelcast.nio.serialization.Data;

/**
 * @author mdogan 6/12/13
 */
public interface LockResource {

    Data getKey();

    boolean isLocked();

    boolean isLockedBy(String owner, int threadId);

    String getOwner();

    boolean isTransactional();

    int getThreadId();

    int getLockCount();

    long getAcquireTime();

    long getRemainingLeaseTime();
}
