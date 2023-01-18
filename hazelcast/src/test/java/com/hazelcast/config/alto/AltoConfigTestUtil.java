package com.hazelcast.config.alto;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.bootstrap.TpcServerBootstrap;

import java.util.List;

import static com.hazelcast.test.Accessors.getNode;
import static org.assertj.core.api.Assertions.assertThat;

public class AltoConfigTestUtil {
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

    public static void assertClientPorts(int eventloopCount, int startInclusive, int endInclusive,
                                         HazelcastInstance... instances) {
        for (HazelcastInstance instance : instances) {
            assertThat(getClientPorts(instance))
                    .allSatisfy(port -> assertThat(port).isBetween(startInclusive, endInclusive))
                    .hasSize(eventloopCount);
        }
    }
}
