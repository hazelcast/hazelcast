package com.hazelcast.internal.tpc.client;

import com.hazelcast.internal.tpc.RequestFuture;
import com.hazelcast.internal.tpc.RpcCore;
import com.hazelcast.internal.tpcengine.iobuffer.IOBuffer;

/**
 * A {@link RpcCore} for client to member communication.
 */
public class ClientRpcCore implements RpcCore {

    @Override
    public RequestFuture<IOBuffer> invoke(int partitionId, IOBuffer request) {
        return null;
    }

    @Override
    public void accept(IOBuffer buffer) {

    }
}
