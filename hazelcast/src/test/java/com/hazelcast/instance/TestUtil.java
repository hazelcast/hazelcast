/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.instance;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.Partition;
import com.hazelcast.core.PartitionService;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.serialization.SerializationService;

import java.io.IOException;
import java.net.DatagramSocket;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static com.hazelcast.test.HazelcastTestSupport.sleepMillis;

public final class TestUtil {

    static final private SerializationService serializationService = new DefaultSerializationServiceBuilder().build();

    private TestUtil() {
    }

    public static Data toData(Object obj) {
        return serializationService.toData(obj);
    }

    public static Object toObject(Data data) {
        return serializationService.toObject(data);
    }

    public static Node getNode(HazelcastInstance hz) {
        HazelcastInstanceImpl impl = getHazelcastInstanceImpl(hz);
        return impl != null ? impl.node : null;
    }

    public static HazelcastInstanceImpl getHazelcastInstanceImpl(HazelcastInstance hz) {
        HazelcastInstanceImpl impl = null;
        if (hz instanceof HazelcastInstanceProxy) {
            impl = ((HazelcastInstanceProxy) hz).original;
        } else if (hz instanceof HazelcastInstanceImpl) {
            impl = (HazelcastInstanceImpl) hz;
        }
        return impl;
    }

    public static void terminateInstance(HazelcastInstance hz) {
        hz.getLifecycleService().terminate();
    }

    public static void warmUpPartitions(HazelcastInstance... instances) {
        for (HazelcastInstance instance : instances) {
            if (instance == null) {
                continue;
            }
            final PartitionService ps = instance.getPartitionService();
            for (Partition partition : ps.getPartitions()) {
                while (partition.getOwner() == null) {
                    sleepMillis(10);
                }
            }
        }
    }

    public static void warmUpPartitions(Collection<HazelcastInstance> instances) {
        for (HazelcastInstance instance : instances) {
            if (instance == null) {
                continue;
            }
            final PartitionService ps = instance.getPartitionService();
            for (Partition partition : ps.getPartitions()) {
                while (partition.getOwner() == null) {
                    sleepMillis(10);
                }
            }
        }
    }

    public static Integer getAvailablePort(int basePort) {
        return getAvailablePorts(basePort, 1).get(0);
    }

    public static List<Integer> getAvailablePorts(int basePort, int portCount) {
        List<Integer> availablePorts = new ArrayList<Integer>();
        int port = basePort;
        for (int i = 0; i < portCount; i++) {
            while (!isPortAvailable(port)) {
                port++;
            }
            availablePorts.add(port++);
        }
        return availablePorts;
    }

    // Checks the DatagramSocket as well to check if the port is available in UDP and TCP.
    public static boolean isPortAvailable(int port) {
        ServerSocket ss = null;
        DatagramSocket ds = null;
        try {
            ss = new ServerSocket(port);
            ss.setReuseAddress(true);
            ds = new DatagramSocket(port);
            ds.setReuseAddress(true);
            return true;
        } catch (IOException e) {

        } finally {
            if (ds != null) {
                ds.close();
            }
            if (ss != null) {
                try {
                    ss.close();
                } catch (IOException e) {

                }
            }
        }

        return false;
    }

    /**
     * Sets or removes (in case value==null) a system property. It's only a helper method, which avoids
     * {@link NullPointerException} thrown from {@link System#setProperty(String, String)} method, when the value is
     * <code>null</code>.
     *
     * @param key   property name
     * @param value property value
     * @return the previous string value of the system property
     */
    public static String setSystemProperty(final String key, final String value) {
        return value == null ? System.clearProperty(key) : System.setProperty(key, value);
    }
}

