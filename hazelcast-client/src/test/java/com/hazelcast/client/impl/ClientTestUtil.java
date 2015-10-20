package com.hazelcast.client.impl;

import com.hazelcast.core.HazelcastInstance;
import org.junit.Ignore;

public final class ClientTestUtil {

    public static HazelcastClientInstanceImpl getHazelcastClientInstanceImpl(HazelcastInstance hz) {
        HazelcastClientInstanceImpl impl = null;
        if (hz instanceof HazelcastClientProxy) {
            impl = ((HazelcastClientProxy) hz).client;
        } else if (hz instanceof HazelcastClientInstanceImpl) {
            impl = (HazelcastClientInstanceImpl) hz;
        }
        return impl;
    }

}
