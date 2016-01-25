package com.hazelcast.osgi.impl;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.osgi.HazelcastOSGiInstance;
import com.hazelcast.osgi.HazelcastOSGiService;

import static org.mockito.Mockito.mock;

public final class HazelcastOSGiTestUtil {

    private HazelcastOSGiTestUtil() {
    }

    public static HazelcastOSGiInstance createHazelcastOSGiInstance(HazelcastInstance instance,
                                                                    HazelcastOSGiService service) {
        return new HazelcastOSGiInstanceImpl(instance, service);
    }

    public static HazelcastOSGiInstance createHazelcastOSGiInstance(HazelcastInstance instance) {
        return createHazelcastOSGiInstance(instance, mock(HazelcastOSGiService.class));
    }

    public static HazelcastOSGiInstance createHazelcastOSGiInstance(HazelcastOSGiService service) {
        return createHazelcastOSGiInstance(mock(HazelcastInstance.class), service);
    }

    public static HazelcastOSGiInstance createHazelcastOSGiInstance() {
        return createHazelcastOSGiInstance(mock(HazelcastInstance.class), mock(HazelcastOSGiService.class));
    }

}
