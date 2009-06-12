/**
 * 
 */
package com.hazelcast.cluster;

import com.hazelcast.impl.MemberImpl;
import com.hazelcast.nio.ConnectionManager;

public class ConnectionCheckCall extends AbstractRemotelyCallable<Boolean> {
    public Boolean call() throws Exception {
        for (MemberImpl member : ClusterManager.lsMembers) {
            if (!member.localMember()) {
                if (ConnectionManager.get().getConnection(member.getAddress()) == null) {
                    return Boolean.FALSE;
                }
            }
        }
        return Boolean.TRUE;
    }
}