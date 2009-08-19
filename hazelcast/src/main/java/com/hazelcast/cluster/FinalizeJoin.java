/**
 * 
 */
package com.hazelcast.cluster;

import com.hazelcast.impl.ListenerManager;

public class FinalizeJoin extends AbstractRemotelyCallable<Boolean> {
    public Boolean call() throws Exception {
        getNode().listenerManager.syncForAdd(getConnection().getEndPoint());
        return Boolean.TRUE;
    }
}