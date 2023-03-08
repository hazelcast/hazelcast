package com.hazelcast.internal.tpcengine.iouring;

import com.hazelcast.internal.tpcengine.net.AcceptRequest;

public class IOUringAcceptRequest implements AcceptRequest {

    final LinuxSocket nativeSocket;

    public IOUringAcceptRequest(LinuxSocket nativeSocket) {
        this.nativeSocket = nativeSocket;
    }

    @Override
    public void close() throws Exception {
        nativeSocket.close();
    }
}
