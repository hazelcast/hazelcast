package com.hazelcast.internal.tpc.client;

import com.hazelcast.internal.tpc.RpcCore;
import com.hazelcast.internal.tpc.TpcRuntime;

public class ClientTpcRuntime implements TpcRuntime {

    private ClientRpcCore rpcCore = new ClientRpcCore();

    @Override
    public int getRequestTimeoutMs() {
        return 0;
    }

    @Override
    public int getPartitionCount() {
        return 0;
    }

    @Override
    public RpcCore getRpcCore() {
        return rpcCore;
    }
}
