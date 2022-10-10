package com.hazelcast.internal.tpc.iouring;

import com.hazelcast.internal.tpc.AcceptRequest;

public class IOUringAcceptRequest implements AcceptRequest {

    final NativeSocket nativeSocket;

    public IOUringAcceptRequest(NativeSocket nativeSocket) {
        this.nativeSocket = nativeSocket;
    }
}
