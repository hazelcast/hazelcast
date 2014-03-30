package com.hazelcast.spi.impl;

import com.hazelcast.instance.MemberImpl;

public interface RemotecallCallback {
    void notify(Object response);

    boolean isCallTarget(MemberImpl leftMember);
}
