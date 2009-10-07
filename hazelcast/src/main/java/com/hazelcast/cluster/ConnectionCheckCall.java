/**
 *
 */
package com.hazelcast.cluster;

import com.hazelcast.impl.MemberImpl;

public class ConnectionCheckCall extends AbstractRemotelyCallable<Boolean> {
    public Boolean call() throws Exception {
        for (MemberImpl member : node.clusterManager.getMembers()) {
            if (!member.localMember()) {
                if (node.connectionManager.getConnection(member.getAddress()) == null) {
                    return Boolean.FALSE;
                }
            }
        }
        return Boolean.TRUE;
    }
}