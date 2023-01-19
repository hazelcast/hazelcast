package com.hazelcast.config.alto;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.bootstrap.TpcServerBootstrap;

import java.util.List;

import static com.hazelcast.test.Accessors.getNode;

public class AltoConfigAccessors {
    public static TpcServerBootstrap getTpcServerBootstrap(HazelcastInstance hz) {
        return getNode(hz).getNodeEngine().getTpcServerBootstrap();
    }

    public static int getEventloopCount(HazelcastInstance hz) {
        return getTpcServerBootstrap(hz).getTpcEngine().eventloopCount();
    }

    public static boolean isTpcEnabled(HazelcastInstance hz) {
        return getTpcServerBootstrap(hz).isEnabled();
    }

    public static AltoSocketConfig getClientSocketConfig(HazelcastInstance hz) {
        return getTpcServerBootstrap(hz).getClientSocketConfig();
    }

    public static List<Integer> getClientPorts(HazelcastInstance hz) {
        return getTpcServerBootstrap(hz).getClientPorts();
    }
}
