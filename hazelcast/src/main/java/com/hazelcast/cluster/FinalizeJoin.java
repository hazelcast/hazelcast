/**
 *
 */
package com.hazelcast.cluster;

public class FinalizeJoin extends AbstractRemotelyCallable<Boolean> {
    public Boolean call() throws Exception {
        getNode().listenerManager.syncForAdd(getConnection().getEndPoint());
        return Boolean.TRUE;
    }
}