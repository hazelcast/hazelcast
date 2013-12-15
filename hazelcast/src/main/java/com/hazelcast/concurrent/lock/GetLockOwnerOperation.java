package com.hazelcast.concurrent.lock;

import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.ObjectNamespace;

public class GetLockOwnerOperation extends BaseLockOperation  {

    public GetLockOwnerOperation() {
    }

    public GetLockOwnerOperation(ObjectNamespace namespace, Data key) {
        super(namespace, key, -1);
    }

    @Override
    public void run() throws Exception {
        response = getLockStore().getLockOwner(key);
    }
}
